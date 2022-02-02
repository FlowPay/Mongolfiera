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
    
    private func handle<T>(_ event: Event<T>) async throws {
        guard let subscription = self.subscriptions[event.topic] else {
            throw Subscription.topicNotFound()
        }
        await withTaskGroup(of: Bool.self) { taskGroup in
                for action in subscription.actions {
                    taskGroup.addTask {
                        do {
                            try await action(event.payload)
                            return true
                        } catch let error {
                            self.logger.report(error: error)
                            return false
                        }
                    }
                }
            try! await self.executionStrategy.exec(taskGroup, on: self.eventLoop)
        }
        do {
            try await self.writeAck(for: event)
            self.logger.debug("Ack sent for \(event._id.objectIDValue?.description ?? "") on \(event.topic)")
        } catch let error {
            self.logger.report(error: error)
            throw error
        }
        
    }
    
    private func writeAck<T>(for event: Event<T>) async throws -> UpdateResult? {
        let query: BSONDocument = ["_id": event._id]
        let ack: BSON = .init(stringLiteral: self.clientName)
        let collection = self.database.collection(event.topic)
        return try await collection.updateOne(
            filter: query,
            update: [ "$push": [ "acks": ack] ]
        )
    }
    
    private func watch<T: Codable>(_ collection: MongoCollection<Document>, of type: T.Type) async throws {
        let watcher =  try await collection.watch()
        for try await notification in watcher {
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
            do {
                let eventChallenge = try await self.eventChallenge(event: event)
                return try await self.handle(eventChallenge)
            } catch let error {
                self.logger.report(error: error)
                try await watcher.kill().get()
                throw error
            }
        }
        do {
            try await watcher.kill().get()
        }catch {
            return try await self.watch(collection, of: type)
        
        }
    }
    
    private func recoverEvents<T:Codable>(on topic: String, as: T.Type) async throws {
        let ack: BSON = .init(stringLiteral: self.clientName)
        let query: BSONDocument = ["acks": ["$nin" : [ack]]]
        
        let cursor = try await self.database.collection(topic).find(query)
        let cursors = try await cursor.toArray()
        try await cursor.kill().get()
        let events : [Event<T>] = cursors.compactMap { event in
            do {
                return try self.decodeDocument(event)
            }
            catch let error {
                self.logger.report(error: error)
                return nil
            }
        }
        return try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                for event in events {
                    return try await self.handle(event)
                }
            }
        }
    }
    
    private func createTTL(collection: MongoCollection<BSONDocument>) async throws -> String {
        var options = IndexOptions()
        options.name = "expire"
        options.expireAfterSeconds = 1
        let index = IndexModel(keys: ["expire": 1], options: options)
        return try await collection.createIndex(index).get()
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
    public func subscribe<T>(to topic: String, action: @escaping (T) async throws -> Void) async throws -> Void where T: Codable {
        let collection = self.database.collection(topic)
        
        let anyFunction: (Any) async throws -> Void = { object in
            return try await action(object as! T)
        }
        
        if self.subscriptions[topic] == nil {
            _ = try await self.createTTL(collection: collection)
            self.subscriptions[topic] = Subscription(topic: topic)
        }
        
        self.subscriptions[topic]?.actions.append(anyFunction)
        
        let recover = try await self.recoverEvents(on: topic, as: T.self)
        let watcher = try await self.watch(collection, of: T.self)
        
        self.subscriptions[topic]?.recovers.append(recover)
        self.subscriptions[topic]?.watchers.append(watcher)
    }
    
    public static func publish<T>(_ object: T, broker: Client, to topic: String) async throws -> Void where T: Codable {
        let collection = broker.database.collection(topic)
        let event = Event(topic: topic, payload: object, expireIn: broker.defaultTTL)
        let document = try broker.encoder.encode(event)
        try await collection.insertOne(document)
        return
    }
    
}
