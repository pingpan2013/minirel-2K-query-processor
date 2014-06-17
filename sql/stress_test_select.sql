
SELECT * FROM students WHERE students.id = 100;

SELECT students.name FROM students WHERE students.gpa > 3.0;

SELECT students.id, students.name, enrollments.name 
FROM students, enrollments WHERE students.id = enrollments.id;
