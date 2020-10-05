//
//  File.swift
//  
//
//  Created by Federico Giuntoli on 04/10/20.
//

import Foundation
import NIO

extension ExecutionStrategy{
    
    func failSlowExec(_ futures: [EventLoopFuture<Void>], on loop: EventLoop) -> EventLoopFuture<Void>{
        return EventLoopFuture.andAllComplete(futures, on: loop)
    }
    
}
