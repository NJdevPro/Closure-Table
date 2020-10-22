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

    # return the depth of the tree between the root and this node
    def ancestors_depth(self, rownum):
        result = self.cursor.execute('SELECT MAX(depth) FROM tree WHERE child = ?', (rownum,));
        return result.fetchone()[0]

    # return the depth of the subtree under the node
    def descendants_depth(self, rownum):
        self.cursor.execute('SELECT MAX(depth) FROM tree WHERE parent = ?', (rownum,));
        return self.cursor.fetchone()[0]

    # return the whole subtree at this node
    def select_descendants(self, rownum):
        self.cursor.execute('''
            SELECT dta.* FROM data_table dta
            JOIN tree t ON (dta.id = t.child) WHERE t.parent = ? AND depth > 0
            ORDER BY depth ASC''', (rownum,))
        return self.cursor.fetchall()

    # return the ancestors of the node
    def select_ancestors(self, rownum):
        self.cursor.execute('''
            SELECT dta.* FROM data_table dta
            JOIN tree t ON (dta.id = t.parent) WHERE t.child = ? AND depth > 0
            ORDER BY depth DESC''', (rownum,))
        return self.cursor.fetchall()

    # return the immediate parent node
    def select_parent(self, rownum):
        self.cursor.execute('''
            SELECT dta.* FROM data_table dta
            JOIN tree t ON (dta.id = t.parent) WHERE t.child = ? AND depth = 1''', (rownum,))
        return self.cursor.fetchone()

    # return the immediate children
    def select_children(self, rownum):
        self.cursor.execute('''
             SELECT dta.* FROM data_table dta
             JOIN tree t ON (dta.id = t.child) WHERE t.parent = ? AND depth = 1''', (rownum,))
        return self.cursor.fetchall()

    # insert the root node
    def insert_root(self, row_parent):
        self.cursor.execute('INSERT INTO tree(parent, child, depth) VALUES (?,?,0)', (row_parent, row_parent))

    # add a new child to a node
    def insert_child(self, row_parent, row_child):
        self.cursor.execute('INSERT INTO tree(parent, child, depth) VALUES (?,?,0)', (row_child, row_child))
        self.link_child(row_parent, row_child)

    # link a child node/subtree to a new parent
    def link_child(self, row_parent, row_child):
        self.cursor.execute('''
            INSERT OR REPLACE INTO tree(parent, child, depth) 
                SELECT p.parent, c.child, p.depth + c.depth + 1
                FROM tree p, tree c
                WHERE p.child = ? AND c.parent = ?''', (row_parent, row_child))

    # unlink a node from its parent
    def unlink_child(self, row_child):
        self.cursor.execute('DELETE FROM tree WHERE child = ? AND depth = 1', (row_child,))

    # unlink a parent node from its child
    def unlink_parent(self, rownum):
        self.cursor.execute('DELETE FROM tree WHERE parent = ? AND depth = 1', (rownum,))

    # delete a subtree
    def delete_descendants(self, rownum):
        self.cursor.execute('DELETE FROM tree WHERE parent = ? AND child <> ?', (rownum, rownum))
