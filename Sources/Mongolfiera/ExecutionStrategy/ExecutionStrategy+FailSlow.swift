//
//  File.swift
//  
//
//  Created by Federico Giuntoli on 04/10/20.
//

import Foundation
import NIO

extension ExecutionStrategy{
    
    struct SlowExecError: Error { }
    
    func failSlowExec(_ futures: TaskGroup<Bool>) async throws {
        var results = [Bool]()
        for await future in futures {
            results.append(future)
        }
        if !(results.allSatisfy{ $0 }) { throw SlowExecError() }
    }
    
}
