//
//  Event.swift
//
//
//  Created by Federico Giuntoli on 11/08/20.
//

import Foundation
import MongoSwift

public struct Event<T>: Codable where T: Codable {
    
    let _id: BSON
    let topic: String
    let timestamp: Double
    let payload: T
    let expire: Date?
    var read: [String] = []
    var acks: [String] = []
    
    public init(topic: String, payload: T, expireIn: TimeInterval = 300) {
        self._id = .objectID()
        self.payload = payload
        self.topic = topic
        self.timestamp = Date().timeIntervalSince1970
        self.expire = Date(timeIntervalSince1970: self.timestamp + expireIn)
    }
    
}
