import Foundation
import Testing

@testable import Xet

@Suite("BG4 Tests")
struct BG4Tests {

    @Test func regroupEmpty() {
        let result = BG4.regroup(Data())
        #expect(result.isEmpty)
    }

    @Test func regroupSingleByte() {
        let result = BG4.regroup(Data([0x42]))
        #expect(result == Data([0x42]))
    }

    @Test func regroupTwoBytes() {
        // With 2 bytes: split=0, rem=2
        // g1Pos = 0 + 1 = 1
        // Input: [a, b] where a is group0[0] remainder, b is group1[0] remainder
        // Output positions: out[0] = a, out[1] = b
        let result = BG4.regroup(Data([0x41, 0x42]))
        #expect(result == Data([0x41, 0x42]))
    }

    @Test func regroupThreeBytes() {
        // With 3 bytes: split=0, rem=3
        // All 3 bytes are remainder bytes
        let result = BG4.regroup(Data([0x41, 0x42, 0x43]))
        #expect(result == Data([0x41, 0x42, 0x43]))
    }

    @Test func regroupFourBytes() {
        // With 4 bytes: split=1, rem=0
        // Each group has exactly 1 byte
        // Input: [g0[0], g1[0], g2[0], g3[0]]
        // Output: interleaved [g0[0], g1[0], g2[0], g3[0]] at positions 0,1,2,3
        let grouped = Data([0x00, 0x01, 0x02, 0x03])
        let result = BG4.regroup(grouped)
        #expect(result == Data([0x00, 0x01, 0x02, 0x03]))
    }

    @Test func regroupSevenBytes() {
        // From the xorb spec: original [0,1,2,3,4,5,6] -> grouped [0,4,1,5,2,6,3]
        // Inverse: grouped [0,4,1,5,2,6,3] -> original [0,1,2,3,4,5,6]
        let grouped = Data([0, 4, 1, 5, 2, 6, 3])
        let result = BG4.regroup(grouped)
        #expect(result == Data([0, 1, 2, 3, 4, 5, 6]))
    }

    @Test func regroupEightBytes() {
        // 8 bytes: split=2, rem=0
        // Groups: [0,1], [2,3], [4,5], [6,7]
        // Output: [0,2,4,6,1,3,5,7] -> interleaved
        let grouped = Data([0x00, 0x04, 0x01, 0x05, 0x02, 0x06, 0x03, 0x07])
        let result = BG4.regroup(grouped)
        #expect(result == Data([0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]))
    }

    @Test func regroupNineBytes() {
        // 9 bytes: split=2, rem=1
        // g0 has 3 bytes (split + 1), others have 2 each
        // Positions: g0=[0,1,2], g1=[3,4], g2=[5,6], g3=[7,8]
        // Interleave: out[0]=g0[0], out[4]=g0[1], out[8]=g0[2]
        //             out[1]=g1[0], out[5]=g1[1]
        //             out[2]=g2[0], out[6]=g2[1]
        //             out[3]=g3[0], out[7]=g3[1]
        let grouped = Data([0, 4, 8, 1, 5, 2, 6, 3, 7])
        let result = BG4.regroup(grouped)
        #expect(result == Data([0, 1, 2, 3, 4, 5, 6, 7, 8]))
    }

    @Test func regroupLargeData() {
        // Test with data large enough to potentially trigger Accelerate path (>= 256 bytes)
        let size = 1024
        var original = Data(count: size)
        for i in 0 ..< size {
            original[i] = UInt8(i % 256)
        }

        // Create grouped version manually
        let split = size / 4
        var grouped = Data(count: size)

        // Group 0: bytes at positions 0, 4, 8, ...
        // Group 1: bytes at positions 1, 5, 9, ...
        // Group 2: bytes at positions 2, 6, 10, ...
        // Group 3: bytes at positions 3, 7, 11, ...
        var g0Idx = 0
        var g1Idx = split
        var g2Idx = split * 2
        var g3Idx = split * 3

        for i in stride(from: 0, to: size, by: 4) {
            grouped[g0Idx] = original[i]
            g0Idx += 1
            if i + 1 < size {
                grouped[g1Idx] = original[i + 1]
                g1Idx += 1
            }
            if i + 2 < size {
                grouped[g2Idx] = original[i + 2]
                g2Idx += 1
            }
            if i + 3 < size {
                grouped[g3Idx] = original[i + 3]
                g3Idx += 1
            }
        }

        let result = BG4.regroup(grouped)
        #expect(result == original)
    }

    @Test func regroupPreservesLength() {
        for length in [0, 1, 2, 3, 4, 5, 7, 8, 15, 16, 100, 255, 256, 257, 1000] {
            let input = Data(repeating: 0x42, count: length)
            let result = BG4.regroup(input)
            #expect(result.count == length, "Length mismatch for input size \(length)")
        }
    }
}
