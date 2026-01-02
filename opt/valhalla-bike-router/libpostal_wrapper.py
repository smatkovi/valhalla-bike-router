#!/usr/bin/env python3
"""
Python ctypes wrapper for libpostal.
Direct binding without needing pypostal compilation.
"""

import ctypes
from ctypes import c_char_p, c_size_t, c_bool, c_uint64, POINTER, Structure
import os
import sys

# Library paths to try
LIBPOSTAL_PATHS = [
    '/opt/libpostal/lib/libpostal.so.1',
    '/opt/libpostal/lib/libpostal.so',
    '/usr/local/lib/libpostal.so.1',
    '/usr/local/lib/libpostal.so',
    '/usr/lib/libpostal.so.1',
    '/usr/lib/libpostal.so',
]

# Data directory
LIBPOSTAL_DATA_DIR = os.environ.get(
    'LIBPOSTAL_DATA_DIR',
    '/home/user/MyDocs/Maps.OSM/postal/global-v1'
)

# Parser data directory (country-specific)
LIBPOSTAL_PARSER_DIR = os.environ.get(
    'LIBPOSTAL_PARSER_DIR',
    '/home/user/MyDocs/Maps.OSM/postal/countries-v1'
)


class LibpostalAddressParserOptions(Structure):
    """libpostal_address_parser_options_t"""
    _fields_ = [
        ('language', c_char_p),
        ('country', c_char_p),
    ]


class LibpostalAddressParserResponse(Structure):
    """libpostal_address_parser_response_t"""
    _fields_ = [
        ('num_components', c_size_t),
        ('labels', POINTER(c_char_p)),
        ('components', POINTER(c_char_p)),
    ]


class LibpostalNormalizeOptions(Structure):
    """libpostal_normalize_options_t"""
    _fields_ = [
        ('languages', POINTER(c_char_p)),
        ('num_languages', c_size_t),
        ('address_components', c_uint64),
        ('latin_ascii', c_bool),
        ('transliterate', c_bool),
        ('strip_accents', c_bool),
        ('decompose', c_bool),
        ('lowercase', c_bool),
        ('trim_string', c_bool),
        ('drop_parentheticals', c_bool),
        ('replace_numeric_hyphens', c_bool),
        ('delete_numeric_hyphens', c_bool),
        ('split_alpha_from_numeric', c_bool),
        ('replace_word_hyphens', c_bool),
        ('delete_word_hyphens', c_bool),
        ('delete_final_periods', c_bool),
        ('delete_acronym_periods', c_bool),
        ('drop_english_possessives', c_bool),
        ('delete_apostrophes', c_bool),
        ('expand_numex', c_bool),
        ('roman_numerals', c_bool),
    ]


class LibpostalWrapper:
    """
    Wrapper for libpostal C library using ctypes.
    
    Usage:
        postal = LibpostalWrapper()
        if postal.setup():
            result = postal.parse_address("Bóné Kálmán utca 6, Budapest")
            print(result)
            # [('bóné kálmán utca', 'road'), ('6', 'house_number'), ('budapest', 'city')]
            postal.teardown()
    """
    
    def __init__(self, lib_path=None, data_dir=None, parser_dir=None):
        self._lib = None
        self._initialized = False
        self._parser_initialized = False
        self._lib_path = lib_path
        self._data_dir = data_dir or LIBPOSTAL_DATA_DIR
        self._parser_dir = parser_dir or LIBPOSTAL_PARSER_DIR
        
    def _load_library(self):
        """Load the libpostal shared library."""
        if self._lib is not None:
            return True
            
        paths_to_try = [self._lib_path] if self._lib_path else LIBPOSTAL_PATHS
        
        for path in paths_to_try:
            if path and os.path.exists(path):
                try:
                    self._lib = ctypes.CDLL(path)
                    self._setup_functions()
                    print(f"[LIBPOSTAL] Loaded library from {path}", file=sys.stderr)
                    return True
                except OSError as e:
                    print(f"[LIBPOSTAL] Failed to load {path}: {e}", file=sys.stderr)
                    continue
        
        print("[LIBPOSTAL] Could not find libpostal.so", file=sys.stderr)
        return False
    
    def _setup_functions(self):
        """Setup function signatures."""
        # Setup/teardown
        self._lib.libpostal_setup.argtypes = []
        self._lib.libpostal_setup.restype = c_bool
        
        self._lib.libpostal_setup_datadir.argtypes = [c_char_p]
        self._lib.libpostal_setup_datadir.restype = c_bool
        
        self._lib.libpostal_setup_parser.argtypes = []
        self._lib.libpostal_setup_parser.restype = c_bool
        
        self._lib.libpostal_setup_parser_datadir.argtypes = [c_char_p]
        self._lib.libpostal_setup_parser_datadir.restype = c_bool
        
        self._lib.libpostal_setup_language_classifier.argtypes = []
        self._lib.libpostal_setup_language_classifier.restype = c_bool
        
        self._lib.libpostal_setup_language_classifier_datadir.argtypes = [c_char_p]
        self._lib.libpostal_setup_language_classifier_datadir.restype = c_bool
        
        self._lib.libpostal_teardown.argtypes = []
        self._lib.libpostal_teardown.restype = None
        
        self._lib.libpostal_teardown_parser.argtypes = []
        self._lib.libpostal_teardown_parser.restype = None
        
        self._lib.libpostal_teardown_language_classifier.argtypes = []
        self._lib.libpostal_teardown_language_classifier.restype = None
        
        # Parser
        self._lib.libpostal_get_address_parser_default_options.argtypes = []
        self._lib.libpostal_get_address_parser_default_options.restype = LibpostalAddressParserOptions
        
        self._lib.libpostal_parse_address.argtypes = [c_char_p, LibpostalAddressParserOptions]
        self._lib.libpostal_parse_address.restype = POINTER(LibpostalAddressParserResponse)
        
        self._lib.libpostal_address_parser_response_destroy.argtypes = [POINTER(LibpostalAddressParserResponse)]
        self._lib.libpostal_address_parser_response_destroy.restype = None
        
        # Expansion (normalize)
        self._lib.libpostal_get_default_options.argtypes = []
        self._lib.libpostal_get_default_options.restype = LibpostalNormalizeOptions
        
        self._lib.libpostal_expand_address.argtypes = [c_char_p, LibpostalNormalizeOptions, POINTER(c_size_t)]
        self._lib.libpostal_expand_address.restype = POINTER(c_char_p)
        
        self._lib.libpostal_expansion_array_destroy.argtypes = [POINTER(c_char_p), c_size_t]
        self._lib.libpostal_expansion_array_destroy.restype = None
    
    def _find_parser_country(self):
        """Find an available country parser directory."""
        if not os.path.exists(self._parser_dir):
            return None
        
        # Look for country directories with address_parser subdirectory
        for iso_code in os.listdir(self._parser_dir):
            parser_dir = os.path.join(self._parser_dir, iso_code, 'address_parser')
            if os.path.isdir(parser_dir):
                # Check if required files exist
                required_files = ['address_parser_crf.dat', 'address_parser_vocab.trie']
                if all(os.path.exists(os.path.join(parser_dir, f)) for f in required_files):
                    print(f"[LIBPOSTAL] Found parser for {iso_code}", file=sys.stderr)
                    return iso_code
        return None
    
    def setup(self, parser=True, language_classifier=True, parser_country=None):
        """
        Initialize libpostal.
        
        Args:
            parser: Setup the address parser
            language_classifier: Setup the language classifier
            parser_country: ISO code for country-specific parser (e.g., 'HU', 'AT')
                          If None, will try to find any available parser
            
        Returns:
            True if setup succeeded
        """
        if not self._load_library():
            return False
        
        data_dir_bytes = self._data_dir.encode('utf-8')
        
        # Setup base library
        if not self._initialized:
            print(f"[LIBPOSTAL] Setting up with data dir: {self._data_dir}", file=sys.stderr)
            if not self._lib.libpostal_setup_datadir(data_dir_bytes):
                print("[LIBPOSTAL] Failed to setup libpostal", file=sys.stderr)
                return False
            self._initialized = True
            print("[LIBPOSTAL] Base setup complete", file=sys.stderr)
        
        # Setup language classifier (needed for parser)
        if language_classifier:
            if not self._lib.libpostal_setup_language_classifier_datadir(data_dir_bytes):
                print("[LIBPOSTAL] Failed to setup language classifier", file=sys.stderr)
                return False
            print("[LIBPOSTAL] Language classifier setup complete", file=sys.stderr)
        
        # Setup parser with country-specific data
        if parser:
            # Find parser country
            if parser_country is None:
                parser_country = self._find_parser_country()
            
            if parser_country is None:
                print("[LIBPOSTAL] No parser data available, parsing disabled", file=sys.stderr)
                return True  # Still return True - expansion works without parser
            
            parser_country_dir = os.path.join(self._parser_dir, parser_country)
            parser_dir_bytes = parser_country_dir.encode('utf-8')
            
            print(f"[LIBPOSTAL] Setting up parser with country dir: {parser_country_dir}", file=sys.stderr)
            if not self._lib.libpostal_setup_parser_datadir(parser_dir_bytes):
                print("[LIBPOSTAL] Failed to setup parser", file=sys.stderr)
                return True  # Still return True - expansion works without parser
            self._parser_initialized = True
            print(f"[LIBPOSTAL] Parser setup complete for {parser_country}", file=sys.stderr)
        
        return True
    
    def teardown(self):
        """Cleanup libpostal resources."""
        if self._lib is None:
            return
            
        if self._parser_initialized:
            self._lib.libpostal_teardown_parser()
            self._parser_initialized = False
            
        if self._initialized:
            self._lib.libpostal_teardown_language_classifier()
            self._lib.libpostal_teardown()
            self._initialized = False
            
        print("[LIBPOSTAL] Teardown complete", file=sys.stderr)
    
    def parse_address(self, address, language=None, country=None):
        """
        Parse an address into components.
        
        Args:
            address: Address string to parse
            language: Optional language code (e.g., 'de', 'hu')
            country: Optional country code (e.g., 'at', 'hu')
            
        Returns:
            List of (component, label) tuples, e.g.:
            [('bóné kálmán utca', 'road'), ('6', 'house_number'), ('budapest', 'city')]
        """
        if not self._parser_initialized:
            if not self.setup():
                return []
        
        # Encode address
        if isinstance(address, str):
            address = address.encode('utf-8')
        
        # Get options
        options = self._lib.libpostal_get_address_parser_default_options()
        
        if language:
            options.language = language.encode('utf-8') if isinstance(language, str) else language
        if country:
            options.country = country.encode('utf-8') if isinstance(country, str) else country
        
        # Parse
        response = self._lib.libpostal_parse_address(address, options)
        
        if not response:
            return []
        
        # Extract results
        results = []
        try:
            for i in range(response.contents.num_components):
                component = response.contents.components[i].decode('utf-8')
                label = response.contents.labels[i].decode('utf-8')
                results.append((component, label))
        finally:
            self._lib.libpostal_address_parser_response_destroy(response)
        
        return results
    
    def expand_address(self, address, languages=None):
        """
        Expand/normalize an address into canonical forms.
        
        Args:
            address: Address string to expand
            languages: Optional list of language codes
            
        Returns:
            List of expanded address strings
        """
        if not self._initialized:
            if not self.setup(parser=False, language_classifier=True):
                return [address]
        
        # Encode address
        if isinstance(address, str):
            address = address.encode('utf-8')
        
        # Get options
        options = self._lib.libpostal_get_default_options()
        
        # TODO: Set languages if provided
        
        # Expand
        num_expansions = c_size_t()
        expansions = self._lib.libpostal_expand_address(address, options, ctypes.byref(num_expansions))
        
        if not expansions:
            return [address.decode('utf-8') if isinstance(address, bytes) else address]
        
        # Extract results
        results = []
        try:
            for i in range(num_expansions.value):
                if expansions[i]:
                    results.append(expansions[i].decode('utf-8'))
        finally:
            self._lib.libpostal_expansion_array_destroy(expansions, num_expansions)
        
        return results if results else [address.decode('utf-8') if isinstance(address, bytes) else address]
    
    def is_available(self):
        """Check if libpostal is available."""
        return self._load_library()
    
    def is_initialized(self):
        """Check if libpostal is initialized."""
        return self._initialized and self._parser_initialized


# Global instance for convenience
_instance = None

def get_instance():
    """Get or create global LibpostalWrapper instance."""
    global _instance
    if _instance is None:
        _instance = LibpostalWrapper()
    return _instance

def parse_address(address, language=None, country=None):
    """
    Convenience function to parse an address.
    
    Args:
        address: Address string
        language: Optional language code
        country: Optional country code
        
    Returns:
        List of (component, label) tuples
    """
    return get_instance().parse_address(address, language, country)

def expand_address(address, languages=None):
    """
    Convenience function to expand an address.
    
    Args:
        address: Address string
        languages: Optional list of language codes
        
    Returns:
        List of expanded address strings
    """
    return get_instance().expand_address(address, languages)

def is_available():
    """Check if libpostal is available."""
    return get_instance().is_available()

def setup(data_dir=None):
    """Initialize libpostal."""
    instance = get_instance()
    if data_dir:
        instance._data_dir = data_dir
    return instance.setup()

def teardown():
    """Cleanup libpostal."""
    get_instance().teardown()


# Test
if __name__ == '__main__':
    print("Testing libpostal wrapper...")
    
    postal = LibpostalWrapper()
    
    if not postal.is_available():
        print("libpostal not found!")
        sys.exit(1)
    
    print(f"libpostal found, initializing...")
    
    if not postal.setup():
        print("Failed to initialize libpostal")
        sys.exit(1)
    
    # Test parsing
    test_addresses = [
        "Bóné Kálmán utca 6, Budapest",
        "Hauptstraße 15, 1010 Wien, Österreich",
        "Drottninggatan 5, Stockholm",
        "781 Franklin Ave Crown Heights Brooklyn NYC NY 11216 USA",
    ]
    
    for addr in test_addresses:
        print(f"\nParsing: {addr}")
        result = postal.parse_address(addr)
        for component, label in result:
            print(f"  {label}: {component}")
    
    # Test expansion
    print("\n\nTesting expansion:")
    test_expand = [
        "Hauptstr. 5",
        "C/ Mayor 10",
        "Ave des Champs-Élysées",
    ]
    
    for addr in test_expand:
        print(f"\nExpanding: {addr}")
        expansions = postal.expand_address(addr)
        for exp in expansions[:5]:  # Show first 5
            print(f"  -> {exp}")
    
    postal.teardown()
    print("\nDone!")
