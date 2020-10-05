//
//  File.swift
//  
//
//  Created by Federico Giuntoli on 04/10/20.
//

import Foundation
import NIO

public enum ExecutionStrategy{
    case failFast
    case failSlow
    
    func exec(_ futures: [EventLoopFuture<Void>], on loop: EventLoop) -> EventLoopFuture<Void>{
        switch self {
        case .failFast:
            return self.failFastExec(futures, on: loop)
        case .failSlow:
            return self.failSlowExec(futures, on: loop)
        }
    }
    
}
