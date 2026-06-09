CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(50),
    salary NUMERIC(10,2),
    join_date DATE
);

INSERT INTO employees (name, department, salary, join_date) VALUES
('Wajid Rahman',   'Engineering',  75000.00, '2023-01-15'),
('Sara Ahmed',     'Marketing',    55000.00, '2022-06-01'),
('Ali Hassan',     'Engineering',  80000.00, '2021-03-20'),
('Priya Sharma',   'HR',           50000.00, '2023-07-10'),
('John Smith',     'Finance',      65000.00, '2020-11-05'),
('Fatima Khan',    'Engineering',  78000.00, '2022-09-15'),
('David Lee',      'Marketing',    52000.00, '2023-03-01'),
('Aisha Malik',    'HR',           48000.00, '2021-08-20'),
('Carlos Rivera',  'Finance',      70000.00, '2020-05-12'),
('Neha Patel',     'Engineering',  82000.00, '2022-12-01');