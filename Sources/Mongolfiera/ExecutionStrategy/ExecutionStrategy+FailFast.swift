//
//  File.swift
//  
//
//  Created by Federico Giuntoli on 04/10/20.
//

import Foundation
import NIO

extension ExecutionStrategy{
    
    func failFastExec(_ futures: TaskGroup<Bool>) async -> Void {
        for await future in futures {
            if !future { futures.cancelAll() }
        }
    }
    
}
