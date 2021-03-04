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
    
    func publishTest(test: TestPayload) -> EventLoopFuture<Void> {
        let client = connect(as: "MongolfieraTest-sender-\(test.test)")
        return client.publish(test, to: "lib/test")
    }
    
    func testGeneric(){
        let exp = expectation(description: "Loading stories")
        
        let testObject = TestPayload()
        
        let client = connect(as: "MongolfieraTest")
        client.subscribe(to: "lib/test"){ (result: TestPayload) in
            if result.test == testObject.test{
                exp.fulfill()
            }
            return self.loop!.next().makeSucceededFuture(())
        }.map{ _ in
            print("Subscription closed")
        }
                
        self.publishTest(test: testObject)
        
        waitForExpectations(timeout: 10)
    }
    
    func testRecover(){
        let exp = expectation(description: "Recovering")
        exp.expectedFulfillmentCount = 2
        
        let client = connect(as: "Mongolfiera-RecoveringTest")
        _ = client.publish("hello", to: "lib/test-recovered").flatMap{
            
            client.subscribe(to: "lib/test-recovered"){ (payload: String) in
                XCTAssertTrue(client.recovering)
                exp.fulfill()
                return self.loop!.next().makeSucceededFuture(())
            }
        }
        self.loop?.execute {
            while client.recovering{
                sleep(2)
            }
            exp.fulfill()
        }
        waitForExpectations(timeout: 10)
    }
    
    func testRecoverWithoutPublish() throws{
        let exp = expectation(description: "Recovering")
        
        let event: EventModel<String> = EventModel(topic: "lib/test-recovered-np", payload: "hello")
        
        let connection = try MongoClient("mongodb://192.168.2.55:30000/?replicaSet=rs0", using: self.loop!)
        let db = connection.db("test")
        _ = db.createCollection("lib/test-recovered-np")
        let collection = db.collection("lib/test-recovered-np")
        
        let data = try BSONEncoder().encode(event)
        _ = try collection.insertOne(data).wait()
        
        try connection.syncClose()
        
        let client = connect(as: "Mongolfiera-RecoveringNPTest")
        
        _ = client.subscribe(to: "lib/test-recovered-np"){ (payload: String) in
            XCTAssertTrue(client.recovering)
            XCTAssertEqual(payload, "hello")
            exp.fulfill()
            return self.loop!.next().makeSucceededFuture(())
        }
        
        waitForExpectations(timeout: 10)

    }
    
    func testConcurrencyRead(){
        let exp = expectation(description: "Wait for acks")
        let attemps = 2
        let payloadTest = TestPayload()
        
        var acksForTest = (1...attemps).map{ index in "MongolfieraTest-\(index)-\(payloadTest.test)" }
        
        exp.expectedFulfillmentCount = attemps
        
        for index in 1...attemps{
            let newLoop = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount).next()
            newLoop.execute{
                let client = self.connect(as: "MongolfieraTest-\(index)-\(payloadTest.test)", on: newLoop)
                self.clients.append(client)
                client.subscribe(to: "lib/test"){ (result: TestPayload) in
                    if result.test == payloadTest.test{
                        client.unsubscribe(from: "lib/test")
                        exp.fulfill()
                    }
                    return self.loop!.next().makeSucceededFuture(())
                }
               
            }
        }
        
        publishTest(test: payloadTest)
        
        
        waitForExpectations(timeout: TimeInterval(attemps + 50))
        
        let query: BSONDocument = [
            "payload":  [
                "test": BSON.init(stringLiteral: payloadTest.test)
            ]
        ]
        
        
        let database: MongoDatabase = try! MongoClient("mongodb://sql1.intra.bancodigitale.com:30000/?replicaSet=rs0", using: self.loop!).db("test")
                
        let collection = database.collection("lib/test")
        let _ = collection.findOne(query).whenComplete{ result in
            print("Found: \(result)")
            
            guard  let document = try? result.get(),
                   let data = try? BSONEncoder().encode(document),
                   var event = try? BSONDecoder().decode(EventModel<TestPayload>.self, from: data) else {
                XCTFail()
                return
            }
            
            XCTAssertEqual(event.payload.test, payloadTest.test)
            event.acks.sort()
            acksForTest.sort()
            XCTAssertEqual(event.acks, acksForTest)
        }
        
    }
    
    func testDelayedPublish(){
        let exp = expectation(description: "Wait for acks")
        let attemps = 20
        let delayMillis = 200
        let payloadTest = TestPayload()
        
        exp.expectedFulfillmentCount = attemps
        
        var uuids: [String] = []
        var uuidsTest: [String] = []
        
        var client: Client? = nil
        
        client = connect(as: "MongolfieraTest-\(payloadTest.test)")
        client!.subscribe(to: "lib/test"){ (result: TestPayload) in
            print("Ricevo \(result.test)")
            
            uuidsTest.append(result.test)
            exp.fulfill()
            return self.loop!.next().makeSucceededFuture(())

        }
        
        var counter = 0
        let anotherLoop = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount).next()
        anotherLoop.scheduleRepeatedTask(initialDelay: TimeAmount.minutes(0), delay: TimeAmount.milliseconds(Int64(delayMillis))) { task in
            
            counter += 1
            
            let testData = TestPayload()
            uuids.append(testData.test)
            
            let client = self.connect(as: "MongolfieraTest-\(counter)-\(payloadTest.test)", on: anotherLoop)
            
            print("Invio \(testData.test)")
            client.publish(testData, to: "lib/test").whenSuccess{
                if counter == attemps {
                    task.cancel()
                }
            }
            
        }
        
        waitForExpectations(timeout: TimeInterval(attemps * min(1, delayMillis/1000) + 60))
        XCTAssertEqual(uuids.sorted(), uuidsTest.sorted())
    }
    
    
    func testWrongModel() throws {
        let exp = expectation(description: "Waiting for approval")
        let client = connect(as: "MongolfieraTest-WrongModel")
        
        client.subscribe(to: "invoice/approval/request") { (event: String) in
            
            return MultiThreadedEventLoopGroup.init(numberOfThreads: 1).next().submit{
                print(event)
                let payload = try JSONDecoder().decode(TestPayload.self, from: event.data(using: .utf8)!)
                print(payload)
                exfp.fulfill()
                return
            }

        }
        
        connect(as: "MongolfieraTest-WrongModel").publish(TestPayload(), to: "invoice/approval/request")
        waitForExpectations(timeout: 10)
    }

}
