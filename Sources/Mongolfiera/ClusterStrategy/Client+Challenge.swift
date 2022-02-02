//
//  File.swift
//  
//
//  Created by Federico Giuntoli on 25/04/21.
//

import Foundation
import NIO
import MongoSwift

extension Client {
    func eventChallenge<T>(event: Event<T>) async throws -> Event<T> {
        
        guard self.clusterStrategy == .allSubscribed
        else { return event }
        
        let clientName = BSON.string(self.clientName)
        
        let filterQuery: BSONDocument = [
            "_id": event._id,
            "read": ["$nin" : [clientName]]
        ]
        
        let updateQuery: BSONDocument = [ "$push": [ "read": clientName] ]
        
        let document = try await self.database.collection(event.topic)
            .findOneAndUpdate(filter: filterQuery, update: updateQuery)
        guard let document = document else {
            let error = ChallengeError(topic: event.topic, eventID: event._id.objectIDValue?.description)
            self.logger.report(error: error)
            throw error
        }
        return try self.decodeDocument(document)
    }
    
}
