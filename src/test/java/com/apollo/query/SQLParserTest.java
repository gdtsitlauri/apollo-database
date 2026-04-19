package com.apollo.query;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SQLParserTest {
    @Test
    void testSqlParse() {
        SQLParser parser = new SQLParser();
        AST ast = parser.parse("select id, name from users where id = 1 order by name desc limit 1");
        assertEquals(AST.StatementType.SELECT, ast.statementType());
        assertEquals("users", ast.tableName());
        assertEquals("id", ast.predicate().column());
        assertEquals("1", ast.predicate().value());
        assertEquals("name", ast.orderBy().column());
        assertEquals(false, ast.orderBy().ascending());
        assertEquals(1, ast.limit());
    }
}
