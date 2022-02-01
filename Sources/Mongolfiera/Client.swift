import Foundation
import NIO
import MongoSwift
import Logging

typealias Document = BSONDocument

public final class Client {
    
    public let connection: MongoClient
    internal let database: MongoDatabase
    internal let eventLoop: EventLoop
    internal let clientName: String
    
    internal var subscriptions: [String : Subscription] = [:]
    
    public var defaultTTL: TimeInterval = 300
    public var executionStrategy: ExecutionStrategy = .failSlow
    public var clusterStrategy: ClusterStrategy = .allSubscribed
    
    public var logger: Logger = .init(label: "MongoDB-Broker")
    
    public var encoder: BSONEncoder = {
        let bsonEncoder = BSONEncoder()
        bsonEncoder.dateEncodingStrategy = .bsonDateTime
        bsonEncoder.uuidEncodingStrategy = .deferredToUUID
        return bsonEncoder
    }()
    
    public var decoder: BSONDecoder = {
        let bsonDecoder = BSONDecoder()
        bsonDecoder.dateDecodingStrategy = .bsonDateTime
        bsonDecoder.uuidDecodingStrategy = .deferredToUUID
        return bsonDecoder
    }()
    
    var watching: Bool {
        subscriptions.values.first(where: {$0.watching}) != nil
    }
    
    var recovering: Bool {
        subscriptions.values.first(where: {$0.recovering}) != nil
    }
    
    public init(connection: MongoClient, dbName: String, as clientName: String, eventLoop: EventLoop, useTransactions: Bool? = true) {
        self.eventLoop = eventLoop
        self.clientName = clientName
        self.connection = connection
        self.database = connection.db(dbName)
    }
    
    public init(dbURI: String, dbName: String, as clientName: String, eventLoop: EventLoop, useTransactions: Bool? = true) throws {
        self.eventLoop = eventLoop
        self.clientName = clientName
        self.connection = try MongoClient(dbURI, using: self.eventLoop)
        self.database = connection.db(dbName)
    }
    
    deinit {
        try? self.connection.syncClose()
    }
    
    
    func decodeDocument<T: Codable>(_ document: BSONDocument) throws -> Event<T> {
        do {
            return try self.decoder.decode(Event<T>.self, from: document.toData())
        }catch {
            
            guard
                let payload = document["payload"],
                let jsonString = (T.self == String.self) ? payload.documentValue?.toExtendedJSONString() : payload.stringValue,
                let newPayload: BSON = (T.self == String.self) ? .string(jsonString) : .document(try .init(fromJSON: jsonString))
            else { throw  DecodingError.typeMismatch(T.self, DecodingError.Context.init(codingPath: [], debugDescription: "")) }
            
            var tempDocument = document
            tempDocument["payload"] = newPayload
            
            return try self.decoder.decode(Event<T>.self, from: tempDocument.toData())
        }
        
    }
    
    private func handle<T>(_ event: Event<T>) -> EventLoopFuture<Void>{
        self.eventLoop.submit{
            guard let subscription = self.subscriptions[event.topic] else {
                throw Subscription.topicNotFound()
            }
            
            return subscription.actions.map{ action in action(event.payload) }
        }
        .flatMap{ promises in
            self.executionStrategy.exec(promises, on: self.eventLoop)
        }
        .flatMap { _ -> EventLoopFuture<UpdateResult?> in
            self.writeAck(for: event)
        }
        .map { (result: UpdateResult?) in
            self.logger.debug("Ack sent for \(event._id.objectIDValue?.description ?? "") on \(event.topic)")
        }
        .flatMapErrorThrowing { error in
            self.logger.report(error: error)
            throw error
        }
    }
    
    private func writeAck<T>(for event: Event<T>) -> EventLoopFuture<UpdateResult?> {
        let query: BSONDocument = ["_id": event._id]
        
        let ack: BSON = .init(stringLiteral: self.clientName)
        let collection = self.database.collection(event.topic)
        return collection.updateOne(
            filter: query,
            update: [ "$push": [ "acks": ack] ]
        )
    }
    
    private func watch<T: Codable>(_ collection: MongoCollection<Document>, of type: T.Type) -> EventLoopFuture<Void> {
        
        collection.watch()
            .flatMap { watcher in
                watcher.forEach{ notification in
                    
                    guard notification.operationType == .insert,
                          let document = notification.fullDocument,
                          let topic = document["topic"]?.stringValue,
                          topic == collection.name,
                          let acks = document["acks"]?.arrayValue,
                          !acks.contains(.string(self.clientName))
                    else {
                        return
                    }
                    
                    let event: Event<T> = try self.decodeDocument(document)
                    
                    try self.eventChallenge(event: event)
                        .flatMap(self.handle)
                        .wait()
                    
                }
                .flatMapError{ error in
                    self.logger.report(error: error)
                    return watcher.kill().flatMapThrowing{ _ in throw error }
                }
                .map{ _ in
                    watcher
                }
            }
            
            .flatMap{ (watcher: ChangeStream<ChangeStreamEvent<BSONDocument>>) in
                watcher.kill()
            }
            .flatMapError{ error in
                //self.logger.report(error: error)
                return self.watch(collection, of: type)
            }
        
    }
    
    private func recoverEvents<T:Codable>(on topic: String, as: T.Type) -> EventLoopFuture<Void>{
        let ack: BSON = .init(stringLiteral: self.clientName)
        let query: BSONDocument = ["acks": ["$nin" : [ack]]]
        
        return self.database.collection(topic)
            .find(query)
            .flatMap { (cursor: MongoCursor<BSONDocument>) in
                cursor.toArray().and(value: cursor)
            }
            .flatMap { (array, cursor) in
                cursor.kill().map{ _ in array}
            }
            .map { events in
                events.compactMap { event in
                    do {
                        return try self.decodeDocument(event)
                    }
                    catch let error {
                        self.logger.report(error: error)
                        return nil
                    }
                }
            }
            .flatMap{ (events: [Event<T>]) in
                let promises = events.map(self.eventChallenge)
                return EventLoopFuture.whenAllComplete(promises, on: self.eventLoop)
            }
            .map{ (results: [Result<Event<T>, Error>]) in
                results.compactMap{
                    guard case let .success(event) = $0
                    else {
                        if case let .failure(error) = $0 {
                            self.logger.report(error: error)
                        }
                        return nil
                    }
                    return event
                }
            }
            .map { (events: [Event<T>]) in
                events.map { self.handle($0) }
            }
            .flatMap { (executions: [EventLoopFuture<Void>]) in
                EventLoopFuture.whenAllComplete(executions, on: self.eventLoop)
            }
            .map { _ in }
    }
    
    private func createTTL(collection: MongoCollection<BSONDocument>) -> EventLoopFuture<String> {
        var options = IndexOptions()
        options.name = "expire"
        options.expireAfterSeconds = 1
        let index = IndexModel(keys: ["expire": 1], options: options)
        return collection.createIndex(index)
    }
    
    public func unsubscribe(from topic: String){
        self.subscriptions.removeValue(forKey: topic)
    }
    
    public func unsubscribe(){
        self.subscriptions.keys.forEach{ key in
            self.unsubscribe(from: key)
        }
    }
    
    @discardableResult
    public func subscribe<T>(to topic: String, action: @escaping (T) -> EventLoopFuture<Void>) -> EventLoopFuture<Void> where T: Codable {
        let collection = self.database.collection(topic)
        
        let anyFunction: (Any) -> EventLoopFuture<Void> = { object in
            return action(object as! T)
        }
        
        if self.subscriptions[topic] == nil {
            _ =  self.createTTL(collection: collection)
            self.subscriptions[topic] = Subscription(topic: topic)
        }
        
        self.subscriptions[topic]?.actions.append(anyFunction)
        
        let recover = self.recoverEvents(on: topic, as: T.self)
        let watcher = self.watch(collection, of: T.self)
        
        self.subscriptions[topic]?.recovers.append(recover)
        self.subscriptions[topic]?.watchers.append(watcher)
        
        return recover.flatMap { watcher }
    }
    
    public func subscribe<T>(to topic: String, action: @escaping (T) async throws -> Void) -> EventLoopFuture<Void> where T: Codable {
        let anyFunction: (T) -> EventLoopFuture<Void> = { object in
            let promise: EventLoopPromise<Void> = self.eventLoop.makePromise()
            promise.completeWithTask {
                try await action(object)
            }
            return promise.futureResult
        }
        return self.subscribe(to: topic, action: anyFunction)
    }
    
    public static func publish<T>(_ object: T, broker: Client, to topic: String) -> EventLoopFuture<Void> where T: Codable {
        
        let collection = broker.database.collection(topic)
        let event = Event(topic: topic, payload: object, expireIn: broker.defaultTTL)
        return broker.eventLoop.submit {
            try broker.encoder.encode(event)
        }
        .flatMap { document in
            collection.insertOne(document)
        }
        .map{ _ in }
    }
    
    public func publish<T>(_ object: T, to topic: String) -> EventLoopFuture<Void>  where T: Codable {
        Client.publish(object, broker: self, to: topic)
    }
    
}
