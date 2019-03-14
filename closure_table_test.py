import unittest
from closure_table import *


# hierarchical test data
mpi = [(0, '2015-06-15', 1, 0),
       (1, '2015-06-16', 1, 1),
       (2, '2015-06-17', 1, 2),
       (3, '2015-06-18', 1, 3),
       (4, '2015-06-19', 1, 4),
       (5, '2015-06-20', 1, 5),
       (6, '2015-06-21', 1, 6),
       (7, '2015-06-22', 1, 7)]


class ClosureTableTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        connexion_string = 'closure.db'
        connexion = sqlite3.connect(connexion_string)
        cls.mpi = ClosureTable(connexion)

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        '''
        Build the tree
        0
        1
        2   3
        4   5 6
            7
        '''
        self.mpi.cursor.executemany("INSERT INTO data_table VALUES (?,?,?,?)", mpi)

        self.mpi.insert_root(0)
        self.mpi.insert_child(0, 1)
        self.mpi.insert_child(1, 2)
        self.mpi.insert_child(1, 3)
        self.mpi.insert_child(2, 4)
        self.mpi.insert_child(3, 5)
        self.mpi.insert_child(3, 6)
        self.mpi.insert_child(5, 7)
        self.mpi.connexion.commit()

    def tearDown(self):
        self.mpi.cursor.execute('delete from tree')
        self.mpi.cursor.execute('delete from data_table')

    def test_a_ancestors_depth(self):
        self.assertEqual(self.mpi.ancestors_depth(0), 0)
        self.assertEqual(self.mpi.ancestors_depth(1), 1)
        self.assertEqual(self.mpi.ancestors_depth(2), 2)
        self.assertEqual(self.mpi.ancestors_depth(3), 2)
        self.assertEqual(self.mpi.ancestors_depth(4), 3)
        self.assertEqual(self.mpi.ancestors_depth(5), 3)
        self.assertEqual(self.mpi.ancestors_depth(6), 3)
        self.assertEqual(self.mpi.ancestors_depth(7), 4)


    def test_b_descendants_depth(self):
        self.assertEqual(self.mpi.descendants_depth(0), 4)
        self.assertEqual(self.mpi.descendants_depth(1), 3)
        self.assertEqual(self.mpi.descendants_depth(2), 1)
        self.assertEqual(self.mpi.descendants_depth(3), 2)
        self.assertEqual(self.mpi.descendants_depth(4), 0)
        self.assertEqual(self.mpi.descendants_depth(5), 1)
        self.assertEqual(self.mpi.descendants_depth(6), 0)
        self.assertEqual(self.mpi.descendants_depth(7), 0)

    def test_c_select_children(self):
        self.assertEqual(self.mpi.select_children(0), [(1, '2015-06-16', 1, 1)])
        self.assertEqual(self.mpi.select_children(3), [(5, '2015-06-20', 1, 5), (6, '2015-06-21', 1, 6)])
        self.assertEqual(self.mpi.select_children(7), [])

    def test_d_select_parent(self):
        self.assertEqual(self.mpi.select_parent(0), None)
        self.assertEqual(self.mpi.select_parent(5), (3, '2015-06-18', 1, 3))

    def test_e_move_subtree(self):
        self.assertEqual(self.mpi.select_children(1), [(2, '2015-06-17', 1, 2), (3, '2015-06-18', 1, 3)])

        self.mpi.unlink_child(3)
        self.mpi.link_child(4, 3)
        self.mpi.connexion.commit()

        self.assertEqual(self.mpi.select_children(1), [(2, '2015-06-17', 1, 2)])
        self.assertEqual(self.mpi.descendants_depth(4), 3)
        self.assertEqual(self.mpi.select_children(4), [(3, '2015-06-18', 1, 3)])
        self.assertEqual(self.mpi.descendants_depth(0), 6)
        self.assertEqual(self.mpi.select_children(3), [(5, '2015-06-20', 1, 5), (6, '2015-06-21', 1, 6)])

    def test_f_delete_descendants(self):
        self.mpi.delete_descendants(3)
        self.assertEqual(self.mpi.descendants_depth(3), 0)
        self.assertEqual(self.mpi.select_children(3), [])
        self.assertEqual(self.mpi.ancestors_depth(3), 2)
        self.assertEqual(self.mpi.descendants_depth(0), 4)

    def test_g_select_descendants(self):
        self.assertEqual(self.mpi.select_descendants(3), mpi[5:8])
        self.assertEqual(self.mpi.select_descendants(0), mpi[1:8])

if __name__ == '__main__':
    unittest.main()
