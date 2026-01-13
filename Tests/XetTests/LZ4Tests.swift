import Foundation
import Testing

@testable import Xet

@Suite("LZ4 Tests")
struct LZ4Tests {

    // MARK: - Raw Block Decompression

    @Test func decompressLiteralsOnly() throws {
        // Token: 0x50 = 5 literals, 0 match length
        let compressed = Data([0x50, 0x68, 0x65, 0x6C, 0x6C, 0x6F])  // "hello"
        let result = try LZ4.decompressRawBlock(compressed, maxOutputSize: 5)
        #expect(result == Data("hello".utf8))
    }

    @Test func decompressWithMatch() throws {
        // "abcabca" - second "abc" is a match
        // Token: 0x30 = 3 literals, then match of length 4 (0 + 4)
        let compressed = Data([
            0x30,  // 3 literals, 0 extra match length (= 4 total)
            0x61, 0x62, 0x63,  // "abc"
            0x03, 0x00,  // offset = 3 (LE)
        ])
        let result = try LZ4.decompressRawBlock(compressed, maxOutputSize: 7)
        #expect(result == Data("abcabca".utf8))
    }

    @Test func decompressEmptyInput() throws {
        let result = try LZ4.decompressRawBlock(Data(), maxOutputSize: 0)
        #expect(result.isEmpty)
    }

    @Test func decompressZeroOutputSize() throws {
        let result = try LZ4.decompressBlock(Data(), uncompressedLength: 0)
        #expect(result.isEmpty)
    }

    @Test func throwsOnInvalidOffset() throws {
        // Token with match but offset pointing outside buffer
        let compressed = Data([
            0x10,  // 1 literal, 0 extra match (= 4 total match)
            0x61,  // "a"
            0x05, 0x00,  // offset = 5 (invalid, only 1 byte in output)
        ])
        #expect(throws: LZ4Error.invalidFrame) {
            _ = try LZ4.decompressRawBlock(compressed, maxOutputSize: 10)
        }
    }

    @Test func throwsOnOutputExceedsMax() throws {
        // Try to decompress more than maxOutputSize allows
        let compressed = Data([0xF0, 0x00])  // 15 literals, needs extension byte
        #expect(throws: LZ4Error.invalidFrame) {
            _ = try LZ4.decompressRawBlock(compressed, maxOutputSize: 10)
        }
    }

    // MARK: - Raw Block Edge Cases

    @Test func decompressZeroLiteralsWithMatch() throws {
        // Token 0x00 = 0 literals, 0 extra match length (= 4 total match)
        // But we need some data first to match against, so use two tokens:
        // First: 4 literals "abcd", then second token: 0 literals + match
        let compressed = Data([
            0x40,  // 4 literals, 0 extra match
            0x61, 0x62, 0x63, 0x64,  // "abcd"
            0x04, 0x00,  // offset = 4
            0x00,  // 0 literals, 0 extra match (= 4 total)
            0x04, 0x00,  // offset = 4
        ])
        let result = try LZ4.decompressRawBlock(compressed, maxOutputSize: 12)
        #expect(result == Data("abcdabcdabcd".utf8))
    }

    @Test func decompressMultiByteLiteralExtension() throws {
        // Token: 0xF0 = 15 literals (needs extension)
        // Extension: 0xFF, 0xFF, 0x0A = 15 + 255 + 255 + 10 = 535 total literals
        var compressed = Data([0xF0, 0xFF, 0xFF, 0x0A])
        let literals = Data(repeating: 0x42, count: 535)  // 535 'B's
        compressed.append(literals)

        let result = try LZ4.decompressRawBlock(compressed, maxOutputSize: 535)
        #expect(result == literals)
    }

    @Test func decompressOverlappingRunPatternOffset1() throws {
        // Start with "A", then match with offset=1 creates "AAAA..."
        // Token: 0x10 = 1 literal, 0 extra match (= 4 total match)
        let compressed = Data([
            0x10,  // 1 literal, 0 extra match
            0x41,  // "A"
            0x01, 0x00,  // offset = 1 (overlapping run)
        ])
        let result = try LZ4.decompressRawBlock(compressed, maxOutputSize: 5)
        #expect(result == Data("AAAAA".utf8))
    }

    @Test func decompressOverlappingRunPatternOffset2() throws {
        // "AB" then offset=2 match creates "ABAB..."
        let compressed = Data([
            0x20,  // 2 literals, 0 extra match
            0x41, 0x42,  // "AB"
            0x02, 0x00,  // offset = 2
        ])
        let result = try LZ4.decompressRawBlock(compressed, maxOutputSize: 6)
        #expect(result == Data("ABABAB".utf8))
    }

    @Test func decompressBoundaryOffsetEqualsOutputCount() throws {
        // 4 literals, then match with offset = 4 (max allowed = output count)
        let compressed = Data([
            0x40,  // 4 literals, 0 extra match
            0x61, 0x62, 0x63, 0x64,  // "abcd"
            0x04, 0x00,  // offset = 4 (exactly output.count)
        ])
        let result = try LZ4.decompressRawBlock(compressed, maxOutputSize: 8)
        #expect(result == Data("abcdabcd".utf8))
    }

    @Test func decompressExtendedMatchLengthMultipleFFBytes() throws {
        // Token 0x1F = 1 literal, 15 extra match (needs extension)
        // Match extension: 0xFF, 0xFF, 0x05 = 15 + 255 + 255 + 5 = 530 extra, + 4 = 534 total match
        let compressed = Data([
            0x1F,  // 1 literal, 15 extra match
            0x41,  // "A"
            0x01, 0x00,  // offset = 1 (run pattern)
            0xFF, 0xFF, 0x05,  // 255 + 255 + 5 = 515 additional
        ])

        let totalMatchLen = 4 + 15 + 255 + 255 + 5  // = 534
        let expectedLen = 1 + totalMatchLen  // 1 literal + 534 match = 535

        let result = try LZ4.decompressRawBlock(compressed, maxOutputSize: expectedLen)
        #expect(result == Data(repeating: 0x41, count: expectedLen))
    }

    // MARK: - Error Path Coverage

    @Test func throwsOnTruncatedInputAtToken() throws {
        // Single token byte with literals expected but no literal bytes
        let compressed = Data([0x10])  // 1 literal expected, but none provided
        #expect(throws: LZ4Error.invalidFrame) {
            _ = try LZ4.decompressRawBlock(compressed, maxOutputSize: 10)
        }
    }

    @Test func throwsOnTruncatedInputDuringLiteralSection() throws {
        // Token says 5 literals but only 3 bytes follow
        let compressed = Data([0x50, 0x61, 0x62, 0x63])  // 5 literals expected, only 3 provided
        #expect(throws: LZ4Error.invalidFrame) {
            _ = try LZ4.decompressRawBlock(compressed, maxOutputSize: 10)
        }
    }

    @Test func throwsOnTruncatedInputBeforeOffsetBytes() throws {
        // Token with match but only one offset byte (need 2)
        let compressed = Data([
            0x10,  // 1 literal, 0 extra match
            0x61,  // "a"
            0x01,  // only 1 offset byte (need 2)
        ])
        #expect(throws: LZ4Error.invalidFrame) {
            _ = try LZ4.decompressRawBlock(compressed, maxOutputSize: 10)
        }
    }

    @Test func throwsOnTruncatedInputDuringLengthExtension() throws {
        // Token: 0xF0 = 15 literals (needs extension), but no extension byte
        let compressed = Data([0xF0])
        #expect(throws: LZ4Error.invalidFrame) {
            _ = try LZ4.decompressRawBlock(compressed, maxOutputSize: 100)
        }
    }

    @Test func throwsOnZeroOffset() throws {
        // offset=0 is invalid
        let compressed = Data([
            0x10,  // 1 literal, 0 extra match
            0x61,  // "a"
            0x00, 0x00,  // offset = 0 (invalid)
        ])
        #expect(throws: LZ4Error.invalidFrame) {
            _ = try LZ4.decompressRawBlock(compressed, maxOutputSize: 10)
        }
    }

    @Test func throwsOnMatchLengthExceedsMaxOutputSize() throws {
        // Valid structure but match would exceed maxOutputSize
        let compressed = Data([
            0x10,  // 1 literal, 0 extra match (= 4 total)
            0x41,  // "A"
            0x01, 0x00,  // offset = 1
        ])
        // 1 literal + 4 match = 5 output, but maxOutputSize = 3
        #expect(throws: LZ4Error.decompressionFailed) {
            _ = try LZ4.decompressRawBlock(compressed, maxOutputSize: 3)
        }
    }

    @Test func throwsDecompressionFailedOnSizeMismatch() throws {
        // Valid block but output doesn't match expected size
        let compressed = Data([0x50, 0x68, 0x65, 0x6C, 0x6C, 0x6F])  // "hello" = 5 bytes
        #expect(throws: LZ4Error.decompressionFailed) {
            _ = try LZ4.decompressBlock(compressed, uncompressedLength: 10)  // expect 10 but get 5
        }
    }

    // MARK: - Frame Detection

    @Test func decompressRawBlockWithoutFrameMagic() throws {
        let compressed = Data([0x10, 0x61])  // 1 literal 'a'
        let raw = try LZ4.decompressBlock(compressed, uncompressedLength: 1)
        #expect(raw == Data([0x61]))
    }

    @Test func detectsAndRejectsIncompleteFrame() {
        let frameMagic = Data([0x04, 0x22, 0x4D, 0x18])
        #expect(throws: LZ4Error.invalidFrame) {
            _ = try LZ4.decompressBlock(frameMagic, uncompressedLength: 4)
        }
    }

    // MARK: - Frame Decoder Tests

    @Test func throwsOnWrongMagicNumber() throws {
        // Wrong magic number (first byte different) - treated as raw block, fails structurally
        let wrongMagic = Data([
            0x05, 0x22, 0x4D, 0x18,  // wrong first byte
            0x64, 0x40, 0xA7,  // rest of header
        ])
        #expect(throws: LZ4Error.invalidFrame) {
            _ = try LZ4.decompressBlock(wrongMagic, uncompressedLength: 5)
        }
    }

    @Test func throwsOnWrongVersionBits() throws {
        // Correct magic but wrong version bits in FLG byte
        let wrongVersion = Data([
            0x04, 0x22, 0x4D, 0x18,  // correct magic
            0x00,  // FLG: version=0 (should be 1, which would be 0x40)
            0x40,  // BD
            0xA7,  // header checksum (wrong but we fail on version first)
        ])
        #expect(throws: LZ4Error.invalidFrame) {
            _ = try LZ4.decompressBlock(wrongVersion, uncompressedLength: 5)
        }
    }

    @Test func throwsOnTruncatedFrameDescriptorAtFLG() throws {
        // Magic only, no FLG byte
        let truncated = Data([0x04, 0x22, 0x4D, 0x18])
        #expect(throws: LZ4Error.invalidFrame) {
            _ = try LZ4.decompressBlock(truncated, uncompressedLength: 5)
        }
    }

    @Test func throwsOnTruncatedFrameDescriptorAtBD() throws {
        // Magic + FLG, no BD byte
        let truncated = Data([0x04, 0x22, 0x4D, 0x18, 0x64])
        #expect(throws: LZ4Error.invalidFrame) {
            _ = try LZ4.decompressBlock(truncated, uncompressedLength: 5)
        }
    }

    @Test func throwsOnTruncatedFrameDescriptorAtHeaderChecksum() throws {
        // Magic + FLG + BD, no header checksum
        let truncated = Data([0x04, 0x22, 0x4D, 0x18, 0x64, 0x40])
        #expect(throws: LZ4Error.invalidFrame) {
            _ = try LZ4.decompressBlock(truncated, uncompressedLength: 5)
        }
    }

    @Test func throwsOnTruncatedFrameAtBlockSize() throws {
        // Complete header but truncated at block size field
        let truncated = Data([
            0x04, 0x22, 0x4D, 0x18,  // magic
            0x64, 0x40, 0xA7,  // FLG, BD, header checksum
            0x05, 0x00,  // incomplete block size (need 4 bytes)
        ])
        #expect(throws: LZ4Error.invalidFrame) {
            _ = try LZ4.decompressBlock(truncated, uncompressedLength: 5)
        }
    }

    // MARK: - Extended Length Encoding

    @Test func decompressExtendedLiteralLength() throws {
        // Token: 0xF0 = 15 literals (needs extension)
        // Extension: 0x05 = +5 = 20 total literals
        var compressed = Data([0xF0, 0x05])
        let literals = Data(repeating: 0x41, count: 20)  // 20 'A's
        compressed.append(literals)

        let result = try LZ4.decompressRawBlock(compressed, maxOutputSize: 20)
        #expect(result == literals)
    }

    @Test func decompressExtendedMatchLength() throws {
        // "aaaa" followed by match of length 4
        let compressed = Data([
            0x40,  // 4 literals, 0 extra match length
            0x61, 0x61, 0x61, 0x61,  // "aaaa"
            0x04, 0x00,  // offset = 4
        ])

        let result = try LZ4.decompressRawBlock(compressed, maxOutputSize: 8)
        #expect(result == Data(repeating: 0x61, count: 8))
    }

    // MARK: - LZ4 CLI Generated Fixtures

    @Test func decompressFrameEmptyData() throws {
        // Generated: echo -n "" | lz4 -c | xxd -i
        let compressed = Data([
            0x04, 0x22, 0x4d, 0x18, 0x64, 0x40, 0xa7, 0x00, 0x00, 0x00, 0x00, 0x05,
            0x5d, 0xcc, 0x02,
        ])
        let result = try LZ4.decompressBlock(compressed, uncompressedLength: 0)
        #expect(result == Data())
    }

    @Test func decompressFrameSmallText() throws {
        // Generated: echo -n "hello" | lz4 -c | xxd -i
        let compressed = Data([
            0x04, 0x22, 0x4d, 0x18, 0x64, 0x40, 0xa7, 0x05, 0x00, 0x00, 0x80, 0x68,
            0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0xf9, 0x77, 0x00, 0xfb,
        ])
        let result = try LZ4.decompressBlock(compressed, uncompressedLength: 5)
        #expect(result == Data("hello".utf8))
    }

    @Test func decompressFrameRepetitivePattern() throws {
        // Generated: printf 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' | lz4 -c | xxd -i
        // 40 'A' characters
        let compressed = Data([
            0x04, 0x22, 0x4d, 0x18, 0x64, 0x40, 0xa7, 0x0b, 0x00, 0x00, 0x00, 0x1f,
            0x41, 0x01, 0x00, 0x0f, 0x50, 0x41, 0x41, 0x41, 0x41, 0x41, 0x00, 0x00,
            0x00, 0x00, 0x83, 0xc5, 0x41, 0x68,
        ])
        let result = try LZ4.decompressBlock(compressed, uncompressedLength: 40)
        #expect(result == Data(repeating: 0x41, count: 40))
    }

    @Test func decompressFrameBinaryData() throws {
        // Generated: printf '\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10' | lz4 -c | xxd -i
        let compressed = Data([
            0x04, 0x22, 0x4d, 0x18, 0x64, 0x40, 0xa7, 0x10, 0x00, 0x00, 0x80, 0x01,
            0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x00, 0x00, 0x00, 0x00, 0x0c, 0xf3, 0x2d, 0xf5,
        ])
        let expected = Data([
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
        ])
        let result = try LZ4.decompressBlock(compressed, uncompressedLength: 16)
        #expect(result == expected)
    }

    @Test func decompressFrameAlphabetWithMatch() throws {
        // Generated: printf 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz' | lz4 -c | xxd -i
        // 52 characters: alphabet repeated twice
        let compressed = Data([
            0x04, 0x22, 0x4d, 0x18, 0x64, 0x40, 0xa7, 0x25, 0x00, 0x00, 0x00, 0xff,
            0x0b, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b,
            0x6c, 0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77,
            0x78, 0x79, 0x7a, 0x1a, 0x00, 0x02, 0x50, 0x76, 0x77, 0x78, 0x79, 0x7a,
            0x00, 0x00, 0x00, 0x00, 0xc3, 0x96, 0xf2, 0xcd,
        ])
        let expected = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
        let result = try LZ4.decompressBlock(compressed, uncompressedLength: 52)
        #expect(result == Data(expected.utf8))
    }
}
