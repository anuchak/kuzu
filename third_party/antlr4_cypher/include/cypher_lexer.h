
// Generated from /home/a8chakra/Repos/shortestpath_kuzu_copy/kuzu/src/antlr4/Cypher.g4 by ANTLR 4.9

#pragma once


#include "antlr4-runtime.h"




class  CypherLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, T__23 = 24, T__24 = 25, T__25 = 26, 
    T__26 = 27, T__27 = 28, T__28 = 29, T__29 = 30, T__30 = 31, T__31 = 32, 
    T__32 = 33, T__33 = 34, T__34 = 35, T__35 = 36, T__36 = 37, T__37 = 38, 
    T__38 = 39, T__39 = 40, T__40 = 41, T__41 = 42, T__42 = 43, T__43 = 44, 
    T__44 = 45, T__45 = 46, GLOB = 47, COPY = 48, FROM = 49, NODE = 50, 
    TABLE = 51, DROP = 52, ALTER = 53, DEFAULT = 54, RENAME = 55, ADD = 56, 
    PRIMARY = 57, KEY = 58, REL = 59, TO = 60, EXPLAIN = 61, PROFILE = 62, 
    UNION = 63, ALL = 64, OPTIONAL = 65, MATCH = 66, UNWIND = 67, CREATE = 68, 
    SET = 69, DELETE = 70, WITH = 71, RETURN = 72, DISTINCT = 73, STAR = 74, 
    AS = 75, ORDER = 76, BY = 77, L_SKIP = 78, LIMIT = 79, ASCENDING = 80, 
    ASC = 81, DESCENDING = 82, DESC = 83, WHERE = 84, SHORTEST = 85, OR = 86, 
    XOR = 87, AND = 88, NOT = 89, INVALID_NOT_EQUAL = 90, MINUS = 91, FACTORIAL = 92, 
    STARTS = 93, ENDS = 94, CONTAINS = 95, IS = 96, NULL_ = 97, TRUE = 98, 
    FALSE = 99, EXISTS = 100, CASE = 101, ELSE = 102, END = 103, WHEN = 104, 
    THEN = 105, StringLiteral = 106, EscapedChar = 107, DecimalInteger = 108, 
    HexLetter = 109, HexDigit = 110, Digit = 111, NonZeroDigit = 112, NonZeroOctDigit = 113, 
    ZeroDigit = 114, RegularDecimalReal = 115, UnescapedSymbolicName = 116, 
    IdentifierStart = 117, IdentifierPart = 118, EscapedSymbolicName = 119, 
    SP = 120, WHITESPACE = 121, Comment = 122, Unknown = 123
  };

  explicit CypherLexer(antlr4::CharStream *input);
  ~CypherLexer();

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames() const override; // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

