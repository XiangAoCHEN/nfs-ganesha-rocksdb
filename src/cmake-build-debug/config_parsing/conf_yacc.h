/* A Bison parser, made by GNU Bison 3.5.1.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2020 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Undocumented macros, especially those whose name start with YY_,
   are private implementation details.  Do not rely on them.  */

#ifndef YY_GANESHA_YY_HOME_LEMON_CODE_NFS_GANESHA_SRC_CMAKE_BUILD_DEBUG_CONFIG_PARSING_CONF_YACC_H_INCLUDED
# define YY_GANESHA_YY_HOME_LEMON_CODE_NFS_GANESHA_SRC_CMAKE_BUILD_DEBUG_CONFIG_PARSING_CONF_YACC_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int ganesha_yydebug;
#endif
/* "%code requires" blocks.  */
#line 49 "/home/lemon/code/nfs-ganesha/src/config_parsing/conf_yacc.y"

/* alert the parser that we have our own definition */
# define YYLTYPE_IS_DECLARED 1


#line 54 "/home/lemon/code/nfs-ganesha/src/cmake-build-debug/config_parsing/conf_yacc.h"

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    _ERROR_ = 258,
    LCURLY_OP = 259,
    RCURLY_OP = 260,
    EQUAL_OP = 261,
    COMMA_OP = 262,
    SEMI_OP = 263,
    IDENTIFIER = 264,
    STRING = 265,
    DQUOTE = 266,
    SQUOTE = 267,
    TOKEN = 268,
    REGEX_TOKEN = 269,
    TOK_PATH = 270,
    TOK_TRUE = 271,
    TOK_FALSE = 272,
    TOK_DECNUM = 273,
    TOK_HEXNUM = 274,
    TOK_OCTNUM = 275,
    TOK_ARITH_OP = 276,
    TOK_V4_ANY = 277,
    TOK_V4ADDR = 278,
    TOK_V4CIDR = 279,
    TOK_V6ADDR = 280,
    TOK_V6CIDR = 281,
    TOK_FSID = 282,
    TOK_NETGROUP = 283
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 55 "/home/lemon/code/nfs-ganesha/src/config_parsing/conf_yacc.y"

  char *token;
  struct config_node *node;

#line 99 "/home/lemon/code/nfs-ganesha/src/cmake-build-debug/config_parsing/conf_yacc.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif



int ganesha_yyparse (struct parser_state *st);
/* "%code provides" blocks.  */
#line 60 "/home/lemon/code/nfs-ganesha/src/config_parsing/conf_yacc.y"


typedef struct YYLTYPE {
  int first_line;
  int first_column;
  int last_line;
  int last_column;
  char *filename;
} YYLTYPE;

# define YYLLOC_DEFAULT(Current, Rhs, N)			       \
    do								       \
      if (N)							       \
	{							       \
	  (Current).first_line	 = YYRHSLOC (Rhs, 1).first_line;       \
	  (Current).first_column = YYRHSLOC (Rhs, 1).first_column;     \
	  (Current).last_line	 = YYRHSLOC (Rhs, N).last_line;	       \
	  (Current).last_column	 = YYRHSLOC (Rhs, N).last_column;      \
	  (Current).filename	 = YYRHSLOC (Rhs, 1).filename;	       \
	}							       \
      else							       \
	{ /* empty RHS */					       \
	  (Current).first_line	 = (Current).last_line	 =	       \
	    YYRHSLOC (Rhs, 0).last_line;			       \
	  (Current).first_column = (Current).last_column =	       \
	    YYRHSLOC (Rhs, 0).last_column;			       \
	  (Current).filename  = NULL;			     /* new */ \
	}							       \
    while (0)

int ganeshun_yylex(YYSTYPE *yylval_param,
		   YYLTYPE *yylloc_param,
		   void *scanner);

int ganesha_yylex(YYSTYPE *yylval_param,
		  YYLTYPE *yylloc_param,
		  struct parser_state *st);

void config_parse_error(YYLTYPE *yylloc_param,
			struct parser_state *st,
			char *format, ...);

void ganesha_yyerror(YYLTYPE *yylloc_param,
		     void *yyscanner,
		     char*);

extern struct glist_head all_blocks;

struct config_node *config_block(char *blockname,
				 struct config_node *list,
				 YYLTYPE *yylloc_param,
				 struct parser_state *st);

void link_node(struct config_node *node);

 struct config_node *link_sibling(struct config_node *first,
				  struct config_node *second);

struct config_node *config_stmt(char *varname,
				struct config_node *exprlist,
				 YYLTYPE *yylloc_param,
				struct parser_state *st);

struct config_node *config_term(char *opcode,
				char *varval,
				enum term_type type,
				 YYLTYPE *yylloc_param,
				struct parser_state *st);


#line 196 "/home/lemon/code/nfs-ganesha/src/cmake-build-debug/config_parsing/conf_yacc.h"

#endif /* !YY_GANESHA_YY_HOME_LEMON_CODE_NFS_GANESHA_SRC_CMAKE_BUILD_DEBUG_CONFIG_PARSING_CONF_YACC_H_INCLUDED  */
