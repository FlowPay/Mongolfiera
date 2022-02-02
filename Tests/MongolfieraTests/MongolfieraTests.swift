import XCTest
import NIO
@testable import Mongolfiera
@testable import MongoSwift

final class MongolFieraTests: XCTestCase {
    
    var loop : EventLoop?
    var clients: [Client] = []
    
    static var allTests = [
        ("generic", testGeneric),
    ]
    
    override func setUp() {
        print("Creo loop")
        self.loop = (MultiThreadedEventLoopGroup.currentEventLoop != nil) ? MultiThreadedEventLoopGroup.currentEventLoop : MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount).next()
        print("loop: \(self.loop!.description)")
    }
    
    override func tearDown() {
        print("Tiro giù")
        
        try? self.loop!.close()
        self.clients.removeAll()
        cleanupMongoSwift()
    }
    
    class TestPayload: Codable{
        let test: String
        var int: Int = 6
        var float: Float = 6.6
        var double: Double = 6.66
        var uuid: UUID = UUID()
        var date: Date = Date()
        var bool: Bool = true
        
        //        let number: Int
        init() {
            self.test = UUID().uuidString
            //            self.number = Int.random(in: 1...1000)
        }
    }
    
    func connect(as name: String = "MongolfieraTest-\(UUID().uuidString)", on loop: EventLoop? = nil) -> Client{
        let client = try! Client(
            dbURI: "mongodb://sql1.intra.bancodigitale.com:30000/?replicaSet=rs0",
            dbName: "test",
            as: name,
            eventLoop: loop != nil ? loop! : MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount).next())
        
        self.clients.append(client)
        return client
    }
    
    func publishTest(test: TestPayload) async throws -> Void {
        let client = connect(as: "MongolfieraTest-sender-\(test.test)")
        return try await Client.publish(test, broker: client, to: "lib/test")
    }
    
    func testGeneric() async throws -> Void {
        let exp = expectation(description: "Loading stories")
        let testObject = TestPayload()
        let client = connect(as: "MongolfieraTest")
        try await client.subscribe(to: "lib/test") {(result: TestPayload) in
            if result.test == testObject.test{
                exp.fulfill()
            }
            return
        }
    }
    
    func testRecover() async throws {
        let exp = expectation(description: "Recovering")
        exp.expectedFulfillmentCount = 2
        let client = connect(as: "Mongolfiera-RecoveringTest")
        try await Client.publish("hello", broker: client, to: "lib/test-recovered")
        try await client.subscribe(to: "lib/test-recovered"){ (payload: String) in
            XCTAssertTrue(client.recovering)
            exp.fulfill()
            return
        }
        while client.recovering {
            sleep(2)
        }
        
        await waitForExpectations(timeout: 10)
    }
    
    
    func testRecoverWithoutPublish() async throws{
        let exp = expectation(description: "Recovering")
        
        let event: Event<String> = Event(topic: "lib/test-recovered-np", payload: "hello")
        
        let connection = self.connect()
        _ = await try connection.database.createCollection("lib/test-recovered-np")
        let collection = connection.database.collection("lib/test-recovered-np")
        
        let data = try BSONEncoder().encode(event)
        _ = try collection.insertOne(data).wait()
                
        let client = connect(as: "Mongolfiera-RecoveringNPTest")
        
        _ = try await client.subscribe(to: "lib/test-recovered-np"){ (payload: String) in
            XCTAssertTrue(client.recovering)
            XCTAssertEqual(payload, "hello")
            exp.fulfill()
            return
        }
        
        await waitForExpectations(timeout: 10)
        
    }
    
    func testDelayedPublish() async throws {
        let exp = expectation(description: "Wait for acks")
        let attemps = 20
        let delayMillis = 200
        let payloadTest = TestPayload()
        exp.expectedFulfillmentCount = attemps
        var uuids: [String] = []
        var uuidsTest: [String] = []
        var client: Client? = nil
        client = connect(as: "MongolfieraTest-\(payloadTest.test)")
        try await client!.subscribe(to: "lib/test") { (result: TestPayload) in
            print("Ricevo \(result.test)")
            uuidsTest.append(result.test)
            exp.fulfill()
            return
        }
        var counter = 0
        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            while counter < 200 {
                counter += 1
                let testData = TestPayload()
                uuids.append(testData.test)
                let client = self.connect(as: "MongolfieraTest-\(counter)-\(payloadTest.test)")
                print("Invio \(testData.test)")
                try await Client.publish(testData, broker: client, to: "lib/test")
                if counter == attemps { taskGroup.cancelAll() }
            }
        }
        await waitForExpectations(timeout: TimeInterval(attemps * min(1, delayMillis/1000) + 60))
        XCTAssertEqual(uuids.sorted(), uuidsTest.sorted())
    }
    
    func testWrongModel() throws {
        let exp = expectation(description: "Waiting for approval")
        let client = connect(as: "MongolfieraTest-WrongModel")
        client.clusterStrategy = .none
    }
    
    func testWrongModel() throws {
        let exp = expectation(description: "Waiting for approval")
        let client = connect(as: "MongolfieraTest-WrongModel")
        client.clusterStrategy = .none
        
        client.subscribe(to: "invoice/approval/request") { (event: String) in
            
            let promise = MultiThreadedEventLoopGroup.init(numberOfThreads: 1).next().submit{
                client.logger.debug(.init(stringLiteral: event))
                let decoder = JSONDecoder()
                decoder.dateDecodingStrategy = .iso8601
                _ = try decoder.decode(TestPayload.self, from: event.data(using: .utf8)!)
                return
            }
            .recover{ error in
                client.logger.report(error: error)
                assertionFailure("\(error)")
                return
            }
            promise.whenComplete{ _ in exp.fulfill()}
            
            return promise
        }
        
        _ = try connect(as: "MongolfieraTest-WrongModel").publish(TestPayload(), to: "invoice/approval/request").wait()
        
        waitForExpectations(timeout: 10)
    }
    
    func testNotInCluster() throws {
        let clientsNumber = 10
        
        let clients: [Client] = (0..<clientsNumber).map { index in
            let client = self.connect(as: "pod")
            client.clusterStrategy = .none
            return client
        }
        
        var counter = 0
        
        clients.forEach{
            _ = $0.subscribe(to: "test/cluster", action: { (_: String) in
                counter = counter + 1
                return self.loop!.makeSucceededVoidFuture()
            })
        }
        
        try self.connect().publish("hellp", to: "test/cluster").wait()
        
        let exp = expectation(description: "Waiting pods")
        Timer.scheduledTimer(withTimeInterval: TimeInterval(clientsNumber) * 0.5, repeats: false) { _ in
            exp.fulfill()
        }
        
        waitForExpectations(timeout: TimeInterval(clientsNumber))
        
        XCTAssertEqual(counter, clientsNumber)
    }
    
    func testCluster() throws {
        let clientsNumber = 10
        
        let clients: [Client] = (0..<clientsNumber).map { _ in
            return self.connect(as: "pod")
        }
        
        var counter = 0
        
        clients.forEach{
            _ = $0.subscribe(to: "test/cluster", action: { (_: String) in
                counter = counter + 1
                return self.loop!.makeSucceededVoidFuture()
            })
        }
        
        try self.connect().publish("hellp", to: "test/cluster").wait()
        
        let exp = expectation(description: "Waiting pods")
        Timer.scheduledTimer(withTimeInterval: 5.0, repeats: false) { _ in
            exp.fulfill()
        }
        
        waitForExpectations(timeout: 10)
        
        XCTAssertEqual(counter, 1)
    }
    
}
