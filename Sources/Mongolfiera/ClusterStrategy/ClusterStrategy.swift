//
//  File.swift
//  
//
//  Created by Federico Giuntoli on 25/04/21.
//

import Foundation
import NIO
import MongoSwift


public enum ClusterStrategy {
    
    case none
    case allSubscribed
//    case oneSubscribed
    
}

public struct ChallengeError: Error {
    let topic: String?
    let eventID: String?
}
