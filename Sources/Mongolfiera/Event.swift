//
//  Event.swift
//  
//
//  Created by Federico Giuntoli on 11/08/20.
//

import Foundation
import MongoSwift

public struct EventModel<T>: Codable where T: Codable {
    
    let _id: BSON
    let topic: String
    let timestamp: Date
    let payload: T
    let expireAt: Date
    var acks: [String] = []
    
    public init(topic: String, payload: T, expireIn: Int = 300) {
        self._id = .objectID()
        self.topic = topic
        self.timestamp = Date()
        self.payload = payload
        self.expireAt = timestamp.addingTimeInterval(TimeInterval(expireIn))
    }
    
}
