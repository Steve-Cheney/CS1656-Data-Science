import sqlite3 as lite
import csv
import re
import pandas as pd
import argparse
import collections
import json
import glob
import math
import os
import requests
import string
import sqlite3
import sys
import time
import xml


#Actors (aid, fname, lname, gender)
#Movies (mid, title, year, rank)
#Directors (did, fname, lname)
#Cast (aid, mid, role)
#Movie_Director (did, mid)

class Movie_db(object):
    def __init__(self, db_name):
        #db_name: "cs1656-public.db"
        self.con = lite.connect(db_name)
        self.cur = self.con.cursor()

    #q0 is an example
    def q0(self):
        query = '''SELECT COUNT(*) FROM Actors'''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

#### Public passed
### !!! PRIVATE FAILED
    def q1(self):
        query = '''
            SELECT a.fname, a.lname
            FROM actors as a NATURAL JOIN cast as c NATURAL JOIN movies as m
            WHERE a.aid in (SELECT aa.aid
                            FROM actors as aa NATURAL JOIN cast as cc NATURAL JOIN movies as mm
                            WHERE mm.year >= 1980 AND mm.year <=1990)
                AND a.aid in (SELECT aa.aid
                                FROM actors as aa NATURAL JOIN cast as cc NATURAL JOIN movies as mm
                                WHERE mm.year >= 2000)
            GROUP BY a.aid
            ORDER BY a.lname, a.fname
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

# COMPLETE
    def q2(self):
        qq = '''
            DROP VIEW IF EXISTS v_year
        '''
        self.cur.execute(qq)
        q0 = '''
            create view v_year as
            SELECT year, rank
            FROM Movies
            WHERE title = "Rogue One: A Star Wars Story"
        '''
        self.cur.execute(q0)
        query = '''
            SELECT m.title, m.year
            FROM Movies as m, v_year
            WHERE m.year = v_year.year AND m.rank > v_year.rank
            ORDER BY m.title
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

#COMPLETE
    def q3(self):
        q0 = '''
            DROP VIEW IF EXISTS distmovs
        '''
        self.cur.execute(q0)
        q1 = '''
            CREATE VIEW distmovs as
            SELECT DISTINCT c.aid, m.title
            FROM cast as c NATURAL JOIN movies as m
        '''
        self.cur.execute(q1)
        query = '''
            SELECT a.fname, a.lname, count(a.aid)
            FROM distmovs NATURAL JOIN actors as a
            WHERE distmovs.title LIKE '%Star Wars%'
            GROUP BY a.fname, a.lname
            ORDER BY count(a.aid) DESC, a.lname, a.fname
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

#COMPLETE
    def q4(self):
        q0 = '''
            DROP VIEW IF EXISTS oldactors
        '''
        self.cur.execute(q0)
        q1 = '''
            CREATE VIEW oldactors as
            SELECT aid
            FROM cast as c NATURAL JOIN MOVIES as m
            WHERE m.year < 1980
        '''
        self.cur.execute(q1)

        q2 = '''
            DROP VIEW IF EXISTS newactors
        '''
        self.cur.execute(q2)
        q3 = '''
            CREATE VIEW newactors as
            SELECT aid
            FROM cast as c NATURAL JOIN MOVIES as m
            WHERE m.year >= 1980
        '''
        self.cur.execute(q3)

        query = '''
            SELECT a1.fname, a1.lname
            FROM oldactors NATURAL JOIN actors as a1, newactors NATURAL JOIN actors as a2
            WHERE oldactors.aid NOT IN newactors
            GROUP BY oldactors.aid
            ORDER BY a1.lname, a1.fname
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

#COMPLETE
    def q5(self):
        query = '''
            SELECT d.fname, d.lname, count(*) as films
            FROM directors as d NATURAL JOIN Movie_Director as m
            GROUP BY d.did
            ORDER BY films DESC, d.lname, d.fname
            LIMIT 10
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

#COMPLETE
    def q6(self):
        query = '''
            SELECT m.title, count(c.aid)
            FROM movies as m NATURAL JOIN cast as c
            GROUP BY m.title
            HAVING count(c.aid) >= (SELECT MIN(num) FROM (
                                            SELECT count(cc.aid) as num
                                            FROM cast as cc
                                            GROUP BY cc.mid
                                            ORDER BY num DESC
                                            LIMIT 10
                                            ))
            ORDER BY count(c.aid) DESC
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

#COMPLETE
    def q7(self):
        q0= '''
            DROP VIEW IF EXISTS actress
        '''
        self.cur.execute(q0)
        q1 = '''
            CREATE view actress as
            SELECT m.mid, c.aid, m.title, count(c.aid) as asaid
            FROM movies as m NATURAL JOIN cast as c NATURAL JOIN actors as a
            WHERE a.gender LIKE 'female'
            GROUP BY m.mid
        '''
        self.cur.execute(q1)
        q2= '''
            DROP VIEW IF EXISTS actor
        '''
        self.cur.execute(q2)
        q3 = '''
            CREATE view actor as
            SELECT m.mid, c.aid, m.title, count(c.aid) as araid
            FROM movies as m NATURAL JOIN cast as c NATURAL JOIN actors as a
            WHERE a.gender LIKE 'male'
            GROUP BY m.mid
        '''
        self.cur.execute(q3)

        query = '''
            SELECT actress.title, actress.asaid, actor.araid
            FROM actress JOIN actor on actress.mid = actor.mid
            where asaid > araid
            GROUP BY actress.title
            ORDER BY actress.title
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q8(self):
        query = '''
            SELECT DISTINCT a.fname, a.lname, count(d.did)
            FROM actors as a NATURAL JOIN cast as c NATURAL JOIN Movie_Director as md NATURAL JOIN directors as d
            WHERE a.fname || a.lname != d.fname || d.lname
            GROUP BY a.aid
            HAVING count(d.did) >= 7
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

#COMPLETE
    def q9(self):
        q0= '''
            DROP VIEW IF EXISTS startyear
        '''
        self.cur.execute(q0)
        q1 = '''
            CREATE view startyear as
            SELECT a.fname, a.lname, MIN(m.year) as minyear, a.aid
            FROM movies as m NATURAL JOIN cast as c NATURAL JOIN actors as a
            GROUP BY a.aid
        '''
        self.cur.execute(q1)
        query = '''
            SELECT startyear.fname, startyear.lname, COUNT(m.mid) as num
            FROM startyear JOIN cast as c on startyear.aid = c.aid JOIN movies as m on c.mid = m.mid
            WHERE m.year = startyear.minyear AND startyear.fname LIKE 'D%'
            GROUP BY c.aid
            ORDER BY num desc, startyear.fname, startyear.lname
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

#COMPLETE
    def q10(self):
        q0= '''
            DROP VIEW IF EXISTS amid
        '''
        self.cur.execute(q0)
        q1 = '''
            CREATE view amid as
            SELECT a.aid, a.fname, a.lname, m.mid
            FROM movies as m NATURAL JOIN cast as c NATURAL JOIN actors as a
        '''
        self.cur.execute(q1)
        q2= '''
            DROP VIEW IF EXISTS dmid
        '''
        self.cur.execute(q2)
        q3 = '''
            CREATE view dmid as
            SELECT d.fname, d.lname, m.mid
            FROM movies as m NATURAL JOIN Movie_Director as md NATURAL JOIN directors as d
        '''
        self.cur.execute(q3)

        query = '''
            SELECT amid.lname, m.title
            FROM amid JOIN dmid on amid.mid = dmid.mid NATURAL JOIN movies as m
            WHERE amid.lname = dmid.lname
            GROUP BY amid.mid
            ORDER BY amid.lname, m.title
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q11(self):
        q0= '''
            DROP VIEW IF EXISTS kb1
        '''
        self.cur.execute(q0)
        q1 = '''
            CREATE view kb1 as
            SELECT DISTINCT a.aid
            FROM actors as a NATURAL JOIN cast as c NATURAL JOIN movies as m
            WHERE a.aid != 1011
            GROUP BY m.mid
            HAVING COUNT(a.lname = 'Bacon' AND a.fname = 'Kevin') >= 1
        '''
        self.cur.execute(q1)

        q2= '''
            DROP VIEW IF EXISTS kb2m
        '''
        self.cur.execute(q2)
        q3 = '''
            CREATE view kb2m as
            SELECT DISTINCT c.aid
            FROM cast as c, kb1
            WHERE c.aid = kb1.aid
            GROUP BY c.mid
        '''
        self.cur.execute(q3)

        query = '''
            SELECT a.fname, a.lname
            FROM kb2m NATURAL JOIN actors as a
            ORDER BY a.lname, a.fname
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

#COMPLETE
    def q12(self):
        query = '''
            SELECT a.fname, a.lname, COUNT(m.mid) as num, AVG(m.rank) as pop
            FROM actors as a NATURAL JOIN cast as c NATURAL JOIN movies as m
            GROUP BY a.aid
            ORDER BY pop desc
            LIMIT 20
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

if __name__ == "__main__":
    task = Movie_db("cs1656-public.db")
    rows = task.q0()
    print(rows)
    print()
    rows = task.q1()
    print(rows)
    print()
    rows = task.q2()
    print(rows)
    print()
    rows = task.q3()
    print(rows)
    print()
    rows = task.q4()
    print(rows)
    print()
    rows = task.q5()
    print(rows)
    print()
    rows = task.q6()
    print(rows)
    print()
    rows = task.q7()
    print(rows)
    print()
    rows = task.q8()
    print(rows)
    print()
    rows = task.q9()
    print(rows)
    print()
    rows = task.q10()
    print(rows)
    print()
    rows = task.q11()
    print(rows)
    print()
    rows = task.q12()
    print(rows)
    print()
