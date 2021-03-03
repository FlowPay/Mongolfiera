//
//  File.swift
//  
//
//  Created by Federico on 03/03/21.
//

import Logging
import Foundation
import MongoSwift

internal extension Logger {
    /// Reports an `Error` to this `Logger`.
    ///
    /// - parameters:
    ///     - error: `Error` to log.
    func report(
        error: Error,
        file: String = #file,
        function: String = #function,
        line: UInt = #line
    ) {
        let reason: String
        let level: Logger.Level = .error
        switch error {
        
        case let localized as LocalizedError:
            reason = localized.localizedDescription
        case let convertible as CustomStringConvertible:
            reason = convertible.description
        case let runtime as MongoRuntimeError:
            reason = "Failure:\n\(runtime.failureReason ?? "")\n\nDescription:\n\(runtime.errorDescription ?? "")\n\nRecovery:\(runtime.recoverySuggestion ?? "")"
        case let user as MongoUserError:
            reason = "Failure:\n\(user.failureReason ?? "")\n\nDescription:\n\(user.errorDescription ?? "")\n\nRecovery:\(user.recoverySuggestion ?? "")"
        case let server as MongoServerError:
            reason = "Failure:\n\(server.failureReason ?? "")\n\nDescription:\n\(server.errorDescription ?? "")\n\nRecovery:\(server.recoverySuggestion ?? "")"
        case let labeled as MongoLabeledError:
            reason = "Failure:\n\(labeled.failureReason ?? "")\n\nDescription:\n\(labeled.errorDescription ?? "")\n\nRecovery:\(labeled.recoverySuggestion ?? "")"
        case let decoding as DecodingError:
            reason = "\(decoding)"
        default:
            reason = "\(error)"
        }
        self.log(
            level: level,
            .init(stringLiteral: reason),
            file: file,
            function: function,
            line: line
        )
    }
}
