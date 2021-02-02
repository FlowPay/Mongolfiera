import Foundation
import NIO
import MongoSwift
import Logging

typealias GenericFunction = (Any) -> EventLoopFuture<Void>
typealias Document = BSONDocument

struct Subscription: Identifiable {
    
    public let topic: String
    
    var recovers: [EventLoopFuture<Void>] = []
    var watchers: [EventLoopFuture<Void>] = []
    var actions: [GenericFunction] = []
    
    var watching: Bool {
        watchers.count > 0
    }
    
    var recovering: Bool {
        recovers.count > 0
    }
    
    public var id: Int {
        return self.topic.hashValue
    }
    
    public init(topic: String){
        self.topic = topic
    }
    
}

extension Subscription{
    private class AlreadyStartedError: Error { }
    private class NotStartedError: Error { }
    public class topicNotFound: Error { }
}

public final class Client {
    
    public let connection: MongoClient
    private let database: MongoDatabase
    private let eventLoop: EventLoop
    private let clientName: String

    private var subscriptions: [String : Subscription] = [:]
    
    public var defaultTTL = 300
    public var executionStrategy: ExecutionStrategy = .failSlow
    
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
    
    private func handle<T>(_ event: EventModel<T>) throws -> EventLoopFuture<Void>{
        guard let subscription = self.subscriptions[event.topic] else{
            throw Subscription.topicNotFound()
        }
        
        let promises = subscription.actions.map{ action in
            return action(event.payload)
        }
        
         return self.executionStrategy.exec(promises, on: self.eventLoop)
            .flatMap{ _ -> EventLoopFuture<UpdateResult?> in
                self.writeAck(for: event)
            }.map{ (result: UpdateResult?) in
                print("Ack sent for \(event.topic)")
            }.flatMapErrorThrowing{ error in
                print(error.localizedDescription)
                throw error
            }
    }
    
    private func writeAck<T>(for event: EventModel<T>) -> EventLoopFuture<UpdateResult?> {
        let query: BSONDocument = ["_id": event._id]
        
        let ack: BSON = .init(stringLiteral: self.clientName)
        let collection = self.database.collection(event.topic)
        return collection.updateOne(
            filter: query,
            update: [ "$push": [ "acks": ack] ]
        )
    }
    
    private func watch<T: Codable>(_ collection: MongoCollection<Document>, of: T.Type) -> EventLoopFuture<Void> {
        
         collection.watch(withFullDocumentType: EventModel<T>.self).flatMap { watcher in
            watcher.forEach{ notification in
                
                guard notification.operationType == .insert,
                      let event = notification.fullDocument,
                      !event.acks.contains(event.topic) else {
                    return
                }
                                
                try! self.handle(event).wait()
                
            }.flatMap{
                watcher.kill()
            }
        }
        
    }
    
    private func recoverEvents<T:Codable>(on topic: String, as: T.Type) -> EventLoopFuture<Void>{
        let ack: BSON = .init(stringLiteral: self.clientName)
        let query: BSONDocument = ["acks": ["$nin" : [ack]]]

        return self.database.collection(topic)
            .find(query)
            .flatMap { (cursor: MongoCursor<BSONDocument>) in
                cursor.forEach{ document in
                    guard let data = try? BSONEncoder().encode(document),
                          let event = try? BSONDecoder().decode(EventModel<T>.self, from: data) else {
                        return
                    }
                    try self.handle(event).wait()
                }.flatMap { _ in
                    cursor.kill()
                }
            }
    }
    
    private func createTTL(collection: MongoCollection<BSONDocument>) -> EventLoopFuture<String> {
        var options = IndexOptions()
        options.name = "expire"
        options.expireAfterSeconds = self.defaultTTL
        let index = IndexModel(keys: ["expireAt": 1], options: options)
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
    
    public func subscribe<T>(to topic: String, action: @escaping (T) -> EventLoopFuture<Void>) -> EventLoopFuture<Void> where T: Codable{
        let collection = self.database.collection(topic)
        
        let anyFunction: (Any) -> EventLoopFuture<Void> = { object in
            return action(object as! T)
        }
        
        if self.subscriptions[topic] == nil {
            _ =  self.createTTL(collection: collection)
            self.subscriptions[topic] = Subscription(topic: topic)
        }
        
        let recover = self.recoverEvents(on: topic, as: T.self)
        let watcher = self.watch(collection, of: T.self)
        
        watcher.whenComplete{ _ in self.subscriptions[topic]?.watchers.removeAll(where: {$0 == watcher}) }
        recover.whenComplete{ _ in self.subscriptions[topic]?.recovers.removeAll(where: {$0 == recover}) }

        self.subscriptions[topic]?.actions.append(anyFunction)
        self.subscriptions[topic]?.recovers.append(recover)
        self.subscriptions[topic]?.watchers.append(watcher)
        
        return watcher
    }
    
    public static func publish<T>(_ object: T, broker: Client, to topic: String) -> EventLoopFuture<Void> where T: Codable {
        
        let collection = broker.database.collection(topic)
        let event = EventModel(topic: topic, payload: object, expireIn: broker.defaultTTL)
        return broker.eventLoop.submit {
            try BSONEncoder().encode(event)
        }
        .flatMap { document in
            collection.insertOne(document).map{ _ in print("Sent message on: \(topic)") }
        }
    }
    
    public func publish<T>(_ object: T, to topic: String) -> EventLoopFuture<Void>  where T: Codable {
        Client.publish(object, broker: self, to: topic)
    }
    
}
