import sqlite3


class ClosureTable:

    def __init__(self, conn):
        self.connexion = conn
        self.cursor = self.build_schema(conn)

    def build_schema(self, conn):
        cursor = conn.cursor()
        cursor.executescript('''
            DROP TABLE IF EXISTS data_table;
            DROP TABLE IF EXISTS tree;
            
            -- Our data
            CREATE TABLE data_table
                    (id INTEGER PRIMARY KEY,
                    updated DATE NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    dummy INTEGER);

            -- Closure table that keeps track of the tree
            CREATE TABLE tree(
                    parent INTEGER NOT NULL DEFAULT 0,
                    child INTEGER NOT NULL DEFAULT 0,
                    depth INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (parent, child),
                    FOREIGN KEY(parent) REFERENCES data_table(id),
                    FOREIGN KEY(child) REFERENCES data_table(id));

            CREATE UNIQUE INDEX tree_idx ON tree(parent, depth, child);
            CREATE UNIQUE INDEX tree_idx2 ON tree(child, parent, depth);
            ''')
        return cursor

    def ancestors_depth(self, rownum):
        result = self.cursor.execute('SELECT MAX(depth) FROM tree WHERE child = ?', (rownum,));
        return result.fetchone()[0]

    def descendants_depth(self, rownum):
        self.cursor.execute('SELECT MAX(depth) FROM tree WHERE parent = ?', (rownum,));
        return self.cursor.fetchone()[0]

    def select_descendants(self, rownum):
        self.cursor.execute('''
            SELECT dta.* FROM data_table dta
            JOIN tree t ON (dta.id = t.child) WHERE t.parent = ? AND depth > 0
            ORDER BY depth ASC''', (rownum,))
        return self.cursor.fetchall()

    def select_ancestors(self, rownum):
        self.cursor.execute('''
            SELECT dta.* FROM data_table dta
            JOIN tree t ON (dta.id = t.parent) WHERE t.child = ? AND depth > 0
            ORDER BY depth DESC''', (rownum,))
        return self.cursor.fetchall()

    def select_parent(self, rownum):
        self.cursor.execute('''
            SELECT dta.* FROM data_table dta
            JOIN tree t ON (dta.id = t.parent) WHERE t.child = ? AND depth = 1''', (rownum,))
        return self.cursor.fetchone()

    def select_children(self, rownum):
        self.cursor.execute('''
             SELECT dta.* FROM data_table dta
             JOIN tree t ON (dta.id = t.child) WHERE t.parent = ? AND depth = 1''', (rownum,))
        return self.cursor.fetchall()

    def insert_root(self, row_parent):
        self.cursor.execute('INSERT INTO tree(parent, child, depth) VALUES (?,?,0)', (row_parent, row_parent))

    def insert_child(self, row_parent, row_child):
        self.cursor.execute('INSERT INTO tree(parent, child, depth) VALUES (?,?,0)', (row_child, row_child))
        self.link_child(row_parent, row_child)

    def link_child(self, row_parent, row_child):
        self.cursor.execute('''
            INSERT OR REPLACE INTO tree(parent, child, depth) 
                SELECT p.parent, c.child, p.depth + c.depth + 1
                FROM tree p, tree c
                WHERE p.child = ? AND c.parent = ?''', (row_parent, row_child))

    def unlink_child(self, row_child):
        self.cursor.execute('DELETE FROM tree WHERE child = ? AND depth = 1', (row_child,))

    def unlink_parent(self, rownum):
        self.cursor.execute('DELETE FROM tree WHERE parent = ? AND depth = 1', (rownum,))

    def delete_descendants(self, rownum):
        self.cursor.execute('DELETE FROM tree WHERE parent = ? AND child <> ?', (rownum, rownum))
