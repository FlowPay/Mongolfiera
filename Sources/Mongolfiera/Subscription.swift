//
//  File.swift
//  
//
//  Created by Federico Giuntoli on 25/04/21.
//

import Foundation
import NIO
import MongoSwift

typealias GenericFunction = (Any) -> EventLoopFuture<Void>

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
