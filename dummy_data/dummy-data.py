import pandas as pd
import random
from datetime import datetime, timedelta
import os

# Create dummy data for Employee table
employee_data = []
for empno in range(1, 1001):
    ename = f'Employee_{empno}'
    sal = random.randint(10000, 50000)
    job = random.choice(['Manager', 'Engineer', 'Analyst', 'Assistant'])
    doj = (datetime.now() - timedelta(days=random.randint(365, 1825))).strftime('%Y-%m-%d')
    deptno = random.randint(1, 10)
    manager_id = random.randint(1, 1000)
    bonus = random.randint(1000, 5000)
    employee_data.append((empno, ename, sal, job, doj, deptno, manager_id, bonus))

# Create dummy data for Department table
department_data = []
for deptno in range(1, 11):
    dname = f'Department_{deptno}'
    location = random.choice(['New York', 'London', 'Paris', 'Tokyo'])
    department_data.append((deptno, dname, location))

# Create dummy data for Salgrade table
salgrade_data = [(1, 10000, 20000), (2, 20001, 30000), (3, 30001, 40000), (4, 40001, 50000), (5, 50001, 60000)]

# Create dummy data for Bonus table
bonus_data = []
for empno in range(1, 1001):
    bonus = random.randint(1000, 5000)
    bonus_data.append((empno, bonus))

# Save dummy data in insert form
employee_inserts = ',\n'.join([f"({empno}, '{ename}', {sal}, '{job}', '{doj}', {deptno}, {manager_id}, {bonus})" for
                               empno, ename, sal, job, doj, deptno, manager_id, bonus in employee_data])
department_inserts = ',\n'.join([f"({deptno}, '{dname}', '{location}')" for deptno, dname, location in department_data])
salgrade_inserts = ',\n'.join([f"({grade}, {min_sal}, {max_sal})" for grade, min_sal, max_sal in salgrade_data])
bonus_inserts = ',\n'.join([f"({empno}, {bonus})" for empno, bonus in bonus_data])

# Save dummy data in CSV format
employee_df = pd.DataFrame(employee_data,
                           columns=['empno', 'ename', 'sal', 'job', 'doj', 'deptno', 'manager_id', 'bonus'])
department_df = pd.DataFrame(department_data, columns=['deptno', 'dname', 'location'])
salgrade_df = pd.DataFrame(salgrade_data, columns=['grade', 'min_sal', 'max_sal'])
bonus_df = pd.DataFrame(bonus_data, columns=['empno', 'bonus'])

# Save dummy data in CSV format
employee_df.to_csv('employee.csv', index=False)
department_df.to_csv('department.csv', index=False)
salgrade_df.to_csv('salgrade.csv', index=False)
bonus_df.to_csv('bonus.csv', index=False)

# Get the path of the notebook folder
notebook_folder = os.path.dirname(os.path.abspath('__file__'))

# Move the CSV files to the notebook folder
os.rename('raw_data/employee.csv', os.path.join(notebook_folder, 'employee.csv'))
os.rename('raw_data/department.csv', os.path.join(notebook_folder, 'department.csv'))
os.rename('raw_data/salgrade.csv', os.path.join(notebook_folder, 'salgrade.csv'))
os.rename('raw_data/bonus.csv', os.path.join(notebook_folder, 'bonus.csv'))

# Print the paths of the CSV files
print('Employee CSV:', os.path.join(notebook_folder, 'employee.csv'))
print('Department CSV:', os.path.join(notebook_folder, 'department.csv'))
print('Salgrade CSV:', os.path.join(notebook_folder, 'salgrade.csv'))
print('Bonus CSV:', os.path.join(notebook_folder, 'bonus.csv'))

# Print the insert statements and file save information
print('Employee Inserts:')
print(employee_inserts)
print('')

print('Department Inserts:')
print(department_inserts)
print('')

print('Salgrade Inserts:')
print(salgrade_inserts)
print('')

print('Bonus Inserts:')
print(bonus_inserts)
print('')

print('Dummy data saved in CSV files:')
print('- employee.csv')
print('- department.csv')
print('- salgrade.csv')
print('- bonus.csv')