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
    
    func exec(_ futures: TaskGroup<Bool>, on loop: EventLoop) async throws -> Void {
        switch self {
        case .failFast:
            return await self.failFastExec(futures)
        case .failSlow:
            return try await self.failSlowExec(futures)
        }
    }
    
}
