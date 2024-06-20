/*
#################################
DATA DREAMER HACKATHON SQLs
#################################
*/

CREATE TABLE org.Employee (
    empno INT PRIMARY KEY,
    ename VARCHAR(50),
    sal INT,
    job VARCHAR(50),
    doj DATE,
    deptno INT,
    manager_id INT,
    bonus INT
);


CREATE TABLE org.Department (
    deptno INT PRIMARY KEY,
    dname VARCHAR(50),
    location VARCHAR(50)
);


CREATE TABLE org.Salgrade (
    grade INT PRIMARY KEY,
    min_sal INT,
    max_sal INT
);

CREATE TABLE org.Bonus (
    empno INT PRIMARY KEY,
    bonus INT
);



################################# DML
INSERT INTO org.Employee (empno, ename, sal, job, doj, deptno, manager_id, bonus)
VALUES
    (1001, 'Rahul Sharma', 50000, 'Manager', '2020-01-01', 10, NULL, NULL),
    (1002, 'Amit Patel', 40000, 'Developer', '2020-02-15', 20, 1001, NULL),
    (1003, 'Sneha Gupta', 35000, 'Analyst', '2020-03-10', 20, 1001, NULL),
    (1100, 'Neha Singh', 45000, 'Manager', '2021-07-01', 30, NULL, NULL);


INSERT INTO org.Department (deptno, dname, location)
VALUES
    (10, 'HR', 'Mumbai'),
    (20, 'IT', 'Bangalore'),
    (30, 'Sales', 'Delhi'),
    (19, 'Marketing', 'Hyderabad');


INSERT INTO org.Salgrade (grade, min_sal, max_sal)
VALUES
    (1, 10000, 20000),
    (2, 20001, 30000),
    (3, 30001, 40000),
    (9, 90001, 100000),
    (10, 100001, 999999);


INSERT INTO org.Bonus (empno, bonus)
VALUES
    (1001, 5000),
    (1002, 2000),
    (1003, 1000),
    (1100, 3500);

#################################
commit;

#################################
select * from org.employee;

select * from org.department;

select * from org.salgrade;

select * from org.bonus;

#################################
rollback;

truncate table org.employee;
truncate table org.department;
truncate table org.salgrade;
truncate table org.bonus;

##################################################################
##################################################################

select * from information_schema.columns;


/*
---  SQL OPERATION
select * from mysql.user;

ALTER USER 'root'@'localhost' IDENTIFIED BY 'rootroot';

*/