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
            raise ValueError(f"Expected {num} bytes but got {len(data)}.")
        return data

    def read_bool(self) -> bool:
        """
        Read a boolean value (4 bytes) from the stream.
        Returns:
            bool: The boolean value read.
        """
        return struct.unpack("I", self.read_bytes(4))[0] != 0

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

    def read_CString(self) -> str:
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