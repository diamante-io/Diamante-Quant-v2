import Foundation

/// Lightweight wallet SDK example for iOS.
public class Wallet {
    public init() {}

    public func getBlock(height: Int) -> Data? {
        // Interact with the light node in production.
        return nil
    }

    public func sync(network: Network) {
        // Delegate to the light node for syncing
    }
}

public protocol Network {
    func streamHeaders() -> AnySequence<(Int, Data)>
}
