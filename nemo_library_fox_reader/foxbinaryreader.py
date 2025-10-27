"""
foxbinaryreader.py: Utility for reading binary data from FOX files.
"""

import struct
import logging
from nemo_library_fox_reader.foxreaderinfo import FOXReaderInfo
from nemo_library_fox_reader.foxstatisticsinfo import IssueType
from typing import BinaryIO


class FoxBinaryReader:
    """
    A helper class for reading various binary data types from a binary stream, specifically for FOX file parsing.
    """
    def __init__(self, stream: BinaryIO, foxReaderInfo: FOXReaderInfo | None = None):
        """
        Initialize the BinaryReader with a binary stream.
        Args:
            stream (BinaryIO): The binary stream to read from.
            foxReaderInfo: Stores statistics information about the implementation of InfoZoom features.
        """
        self.stream = stream
        self.foxReaderInfo = foxReaderInfo
        # logging.info(f"BinaryReader __init__ foxReaderInfo={self.foxReaderInfo}")


    def read_bytes(self, num: int) -> bytes:
        """
        Read a specified number of bytes from the stream.
        Args:
            num (int): Number of bytes to read.
        Returns:
            bytes: The bytes read from the stream.
        Raises:
            ValueError: If the expected number of bytes cannot be read.
        """
        data = self.stream.read(num)

        if len(data) != num:
            logging.debug(f"Expected {num} bytes but got  {len(data)}: {data}")
            raise ValueError(f"Expected {num} bytes but got {len(data)}.")
        return data

    def read_bool(self) -> bool:
        """
        Read a boolean value (4 bytes) from the stream.
        Returns:
            bool: The boolean value read.
        """
        return struct.unpack("I", self.read_bytes(4))[0] != 0

    def read_byte(self) -> int:
        """
        Read a 1-byte  integer from the stream.
        Returns:
            int: The short integer value read.
        """
        bytes = self.read_bytes(1)
        return bytes[0]

    def read_short_int(self) -> int:
        """
        Read a 2-byte short integer from the stream.
        Returns:
            int: The short integer value read.
        """
        return struct.unpack("h", self.read_bytes(2))[0]

    def read_int(self) -> int:
        """
        Read a 4-byte integer from the stream.
        Returns:
            int: The integer value read.
        """
        return struct.unpack("I", self.read_bytes(4))[0]

    def read_double(self) -> float:
        """
        Read an 8-byte double from the stream.
        Returns:
            float: The double value read.
        """
        return struct.unpack("d", self.read_bytes(8))[0]

    def read_tchar(self) -> str:
        """
        Read a TCHAR (2 bytes, UTF-16LE) from the stream.
        Returns:
            str: The decoded character.
        """
        return self.read_bytes(2).decode("utf-16le")

    def read_CString_old(self) -> str:
        """
        Read a CString (UTF-16LE) from the stream.
        Returns:
            str: The decoded string.
        Raises:
            ValueError: If the BOM does not match UTF-16LE.
        """
        prefix = self.read_bytes(4)
        utf16le_bom = prefix[0:2]
        if utf16le_bom != b"\xff\xfe":
            raise ValueError(f"BOM does not match UTF-16LE. Found: {utf16le_bom.hex()}")
        length = prefix[3]
        return self.read_bytes(2 * length).decode("utf-16le")

    def read_CString(self) -> str:
        """
        Read a CString (UTF-16LE) from the stream.
        Returns:
            str: The decoded string.
        Raises:
            ValueError: If the BOM does not match UTF-16LE.
        """
        utf16le_bom = self.read_bytes(2) 
        if utf16le_bom != b"\xff\xfe":
            raise ValueError(f"BOM does not match UTF-16LE. Found: {utf16le_bom.hex()}")
        length = self.read_CString_length()
        return self.read_bytes(2 * length).decode("utf-16le")
        
    def read_CString_length(self) -> int:
        """
        Read a variable-length integer like MFC's AfxReadStringLength().
        Returns:
            int: The decoded string length.
        """
        first = self.read_byte()
        if first < 0x80:
            # 1-byte length (0–127)
            return first
        elif first < 0xC0:
            # 2-byte length
            second = self.read_byte()
            return ((first & 0x3F) << 8) | second
        elif first < 0xE0:
            # 4-byte length (3 extra bytes)
            second = self.read_byte()
            third = self.read_byte()
            fourth = self.read_byte()
            return ((first & 0x1F) << 24) | (second << 16) | (third << 8) | fourth
        elif first == 0xFF:
            # 4-Byte Zahl Little-Endian
            return int.from_bytes(self.read_bytes(4), "little")
        else:
            raise ValueError(f"Invalid length prefix: {first:02X}")
    
## code by co-pilot, not working properly
    # def read_CString(self) -> str:
    #     """
    #     Read a CString (UTF-16LE) from the stream in a robust way.
    #     Supports multiple header encodings observed in FOX files:
    #       - BOM present at bytes 0-1, length in bytes 2-3 (16-bit little-endian length)
    #       - No BOM, length in bytes 0-1 (16-bit little-endian)
    #       - Short form length in prefix[3] (1-byte length)
    #     Returns:
    #         str: The decoded string (empty string for length 0 or on unrecoverable errors).
    #     """
    #     # remember stream position so we can rewind on decode failures (if seekable)
    #     stream_pos = None
    #     try:
    #         stream_pos = self.stream.tell()
    #     except Exception:
    #         stream_pos = None
    #     data = None

    #     prefix = self.read_bytes(4)
    #     utf16le_bom = prefix[0:2]

    #     # If BOM present, use the (bytes 2-3) 16-bit length
    #     if utf16le_bom == b"\xff\xfe":
    #         long_length = int.from_bytes(prefix[2:4], byteorder="little", signed=False)
    #         if long_length == 0:
    #             return ""
    #         data = b""
    #         try:
    #             data = self.read_bytes(2 * long_length)
    #             return data.decode("utf-16le")
    #         except UnicodeDecodeError as ude:
    #             logging.debug(f"Unicode decode failed for long form CString (len={long_length}): {ude}")
    #             # attempt to rewind and try short form if possible
    #             if stream_pos is not None and hasattr(self.stream, "seek"):
    #                 try:
    #                     self.stream.seek(stream_pos)
    #                     prefix2 = self.read_bytes(4)
    #                     short_length = prefix2[3]
    #                     if short_length == 0:
    #                         return ""
    #                     data2 = self.read_bytes(2 * short_length)
    #                     return data2.decode("utf-16le")
    #                 except Exception as e:
    #                     logging.debug(f"Fallback short-form CString read failed: {e}")
    #             # last resort: decode with replacement to avoid exceptions
    #             try:
    #                 return data.decode("utf-16le", errors="replace")
    #             except Exception:
    #                 return ""

    #     # No BOM - try alternate interpretations. Many files use prefix[0:2] as the length
    #     logging.debug(f"No UTF-16LE BOM in CString prefix: {prefix.hex()} - trying alternate length interpretations")

    #     len_from_start = int.from_bytes(prefix[0:2], byteorder="little", signed=False)
    #     if len_from_start == 0:
    #         return ""

    #     # If stream is seekable, try length interpretations rewinding between attempts
    #     if stream_pos is not None and hasattr(self.stream, "seek"):
    #         for attempt_name, length_val in (("len_from_start", len_from_start), ("short", prefix[3])):
    #             try:
    #                 self.stream.seek(stream_pos)
    #                 # re-read header
    #                 _ = self.read_bytes(4)
    #                 if length_val == 0:
    #                     return ""
    #                 bts = self.read_bytes(2 * int(length_val))
    #                 try:
    #                     return bts.decode("utf-16le")
    #                 except UnicodeDecodeError:
    #                     logging.debug(f"Attempt {attempt_name} decode failed (len={length_val})")
    #                     continue
    #             except Exception as e:
    #                 logging.debug(f"Attempt {attempt_name} failed to read/decode: {e}")
    #                 continue

    #     # Not seekable - best-effort using the prefix we already consumed
    #     try:
    #         data = self.read_bytes(2 * len_from_start)
    #         try:
    #             return data.decode("utf-16le")
    #         except UnicodeDecodeError:
    #             logging.debug(f"Decode failed for len_from_start={len_from_start}; trying short form if possible")
    #     except Exception as e:
    #         logging.debug(f"Could not read bytes for len_from_start={len_from_start}: {e}")

    #     # try short form using prefix[3]
    #     try:
    #         short_len = prefix[3]
    #         data2 = self.read_bytes(2 * short_len)
    #         return data2.decode("utf-16le")
    #     except Exception:
    #         pass

    #     # Last resort: decode what we have with replacement or return empty
    #     try:
    #         if 'data' in locals() and data is not None:
    #             return data.decode("utf-16le", errors="replace")
    #     except Exception:
    #         pass
    #     return ""




    def read_compressed_string(self) -> str:
        """
        Read a compressed UTF-8 string from the stream.
        Returns:
            str: The decoded string.
        Raises:
            ValueError: If the string length is invalid.
        """
        length = self.read_short_int()
        if length < 0:
            raise ValueError("Invalid length for compressed string.")
        return self.read_bytes(length).decode("utf-8")

    def unpack_n_byte_values(self, data: bytes, n: int, byteorder="little", signed=False) -> list[int]:
        """
        Unpack a byte array into a list of integers, each of n bytes.
        Args:
            data (bytes): Byte array to unpack.
            n (int): Number of bytes per integer.
            byteorder (str): Byte order (default 'little').
            signed (bool): Whether values are signed (default False).
        Returns:
            list[int]: List of unpacked integer values.
        Raises:
            ValueError: If the byte array length is not a multiple of n.
        """
        if len(data) % n != 0:
            raise ValueError(f"Data length ({len(data)}) is not a multiple of {n}.")
        return [int.from_bytes(data[i:i+n], byteorder=byteorder, signed=signed)
                for i in range(0, len(data), n)]
        
    def read_compressed_value(self, attribute_name: str, value_store: list[str], last_value: str, bytes_per_index: int) -> str:
        """
        Reads a compressed string value, possibly reusing prefix from the last value.
        Args:
            last_value (str): The previous value for partial compression.
            bytes_per_index (int): Number of bytes per index.
        Returns:
            str: The decompressed value.
        Raises:
            NotImplementedError: If multi-value decoding is encountered.
        """
        i_length_in_bytes = self.read_short_int()

        if i_length_in_bytes == 0:
            return ""

        if i_length_in_bytes < 0:
            i_length_in_bytes = -i_length_in_bytes - 1
            num_identical_chars = struct.unpack("B", self.read_bytes(1))[0]
            start_str = last_value[:num_identical_chars]
            end_str = self.read_bytes(i_length_in_bytes).decode("utf-8")
            return start_str + end_str

        else:
            char_buffer = self.read_bytes(i_length_in_bytes)
            temp = char_buffer.decode("utf-8", errors="ignore")
            if temp.startswith("%"):
                if self.foxReaderInfo:
                    if (attribute_name not in self.foxReaderInfo.attributes_with_images_shown):
                        self.foxReaderInfo.attributes_with_images_shown.append(attribute_name)
                        self.foxReaderInfo.add_issue(IssueType.IMAGESSHOWN, attribute_name)

            if temp.startswith("#") and len(temp) > 1 and temp[1].isnumeric:
                if self.foxReaderInfo:
                    if (attribute_name not in self.foxReaderInfo.attributes_with_sort_order_used):
                        self.foxReaderInfo.attributes_with_sort_order_used.append(attribute_name)
                        self.foxReaderInfo.add_issue(IssueType.SORTORDERSUSED, attribute_name)

            if temp.startswith("~|"):
                if self.foxReaderInfo:
                    if (attribute_name not in self.foxReaderInfo.attributes_with_html_links_used):
                        self.foxReaderInfo.attributes_with_html_links_used.append(attribute_name)
                        self.foxReaderInfo.add_issue(IssueType.HTMLLINKSUSED, attribute_name)

            if temp.startswith("|"):
                if self.foxReaderInfo:
                    if (attribute_name not in self.foxReaderInfo.attributes_with_multiple_values):
                        self.foxReaderInfo.attributes_with_multiple_values.append(attribute_name)
                        self.foxReaderInfo.add_issue(IssueType.MULTIPLEVALUES, attribute_name)
                # else:
                #     logging.info("FOXBinaryReader foxReaderInfo is None")

                # Multi-value attributes starts with a "|"
                # Because they are not supported in Nemo right now they are changed to single values by concatenating the values
                try:
                    concatenated_values = "| "

                    #after the "|" the number of values are written in the first byte followed by the indices of the value in the value store
                    num_values = ord(temp[1])
                    
                    for i in range(2,num_values + 2):
                        index_in_value_store = ord(temp[i])
                        if (index_in_value_store < len(value_store)):
                            multiple_value = value_store[index_in_value_store]
                            concatenated_values += multiple_value + " | "
                        else:
                            concatenated_values += f"invalid index {index_in_value_store} #={len(value_store)} | "

                    return concatenated_values
                except Exception:
                    return temp
                

            return temp        
        
    def read_color_scheme(self) -> dict:
        """
        Reads the color scheme information from the FOX file.
        Returns:
            dict: Dictionary containing color scheme data.
        """
        Result = {}
        Result["BarColor"] = self.read_int()
        Result["OddColor"] = self.read_int()
        Result["DummyColor"] = self.read_int()
        Result["EvenColor"] = self.read_int()
        Result["DummyColor"] = self.read_int()
        Result["BackgroundColor"] = self.read_int()
        Result["bColorShadingOverview"] = self.read_bool()
        Result["bColorShadingTable"] = self.read_bool()
        Result["bDummyBool"] = self.read_bool()
        Result["bInitialized"] = self.read_bool()
        return Result        
    
    def read_sorted_characters(self):
        """
        Reads sorted character information from the FOX file (used for sort order).
        """
        NR_OF_CHARACTERS = 65536
        SortedCharacters = [0] * NR_OF_CHARACTERS
        i = 0
        while i < NR_OF_CHARACTERS:
            cChar = self.read_short_int()
            SortedCharacters[i] = cChar
            if i > 0 and cChar == SortedCharacters[i - 1]:
                i -= 1
                cEndOfChainChar = self.read_short_int()
                temp_from = cChar + 1
                temp_to = cEndOfChainChar + 1
                for c in range(temp_from, temp_to):
                    i += 1
                    SortedCharacters[i] = c
            i = i + 1
        n_gelesen_zur_info = 0
        while True:
            cChar = self.read_short_int()
            cPlaceholder = self.read_short_int()
            n_gelesen_zur_info = n_gelesen_zur_info + 1
            if n_gelesen_zur_info % 10000 == 0:
                print(f"{n_gelesen_zur_info} Zeichen...")
            if cChar == 0 and cPlaceholder == 0:
                break