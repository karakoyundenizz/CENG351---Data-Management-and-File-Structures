-- --------------------------------------------------------
-- Part 1: CREATE TABLE statements
-- --------------------------------------------------------

-- Parent tables (must be created first)
drop table Student, Professor, Course, Transcript, Teaching;
CREATE TABLE Student (
    StudentId INTEGER PRIMARY KEY,
    Name VARCHAR(100),
    Address VARCHAR(255),
    Status VARCHAR(50),
    Gpa DECIMAL(3, 2)
);

CREATE TABLE Professor (
    ProfessorId INTEGER PRIMARY KEY,
    Name VARCHAR(100),
    DeptID VARCHAR(10),
    Age INTEGER
);

CREATE TABLE Course (
    CourseId VARCHAR(10) PRIMARY KEY,
    DeptID VARCHAR(10),
    CourseName VARCHAR(255)
);

-- Child tables (with Foreign Keys referencing parent tables)

CREATE TABLE Transcript (
    StudentId INTEGER,
    CourseId VARCHAR(10),
    Semester VARCHAR(10),
    Score INTEGER,
    PRIMARY KEY (StudentId, CourseId, Semester),
    FOREIGN KEY (StudentId) REFERENCES Student(StudentId),
    FOREIGN KEY (CourseId) REFERENCES Course(CourseId)
);

CREATE TABLE Teaching (
    CourseId VARCHAR(10),
    Semester VARCHAR(10),
    ProfessorId INTEGER,
    PRIMARY KEY (CourseId, Semester),
    FOREIGN KEY (CourseId) REFERENCES Course(CourseId),
    FOREIGN KEY (ProfessorId) REFERENCES Professor(ProfessorId)
);

-- --------------------------------------------------------
-- Part 2: INSERT INTO statements
-- --------------------------------------------------------

-- Insert data into parent tables first

INSERT INTO Student (StudentId, Name, Address, Status, Gpa) VALUES
(1, 'Martin Prince', 'Eskişehir yolu, Ankara', 'Senior', 2.5),
(2, 'Lisa Simpson', 'İstanbul yolu, Ankara', 'Junior', 3.5),
(3, 'Milhouse Van Houten', 'Ümitköy, Ankara', 'Senior', 2.7),
(4, 'Nelson Muntz', 'Keçiören, Ankara', 'Senior', 2.0),
(5, 'Ralph Wiggum', 'Yenimahalle, Ankara', 'Freshman', 2.5),
(6, 'Todd Flanders', 'Yenimahalle, Ankara', 'Sophomore', 2.8);

INSERT INTO Professor (ProfessorId, Name, DeptID, Age) VALUES
(1, 'Waylon Smithers', 'CENG', 35),
(2, 'Edna Krabappel', 'EE', 42),
(3, 'Hans Moleman', 'CENG', 63),
(4, 'Joe Quimby', 'ME', 45),
(5, 'Julius Hibbert', 'METE', 50),
(6, 'Troy McClure', 'EE', 35);

INSERT INTO Course (CourseId, DeptID, CourseName) VALUES
('CENG100', 'CENG', 'Computer Engineering Orientation'),
('CENG230', 'CENG', 'Introduction to C Programming'),
('CENG223', 'CENG', 'Discrete Computational Structures'),
('EE201', 'EE', 'Circuit Theory I'),
('EE213', 'EE', 'Electrical Circuits Laboratory'),
('METE201', 'METE', 'Materials Science I'),
('METE202', 'METE', 'Materials Science II'),
('ME202', 'ME', 'Manufacturing Technologies');

-- Insert data into child tables last

INSERT INTO Transcript (StudentId, CourseId, Semester, Score) VALUES
(1, 'EE213', 'F2013', 20),
(1, 'EE201', 'S2014', 60),
(1, 'CENG230', 'S2014', 65),
(2, 'CENG230', 'F2013', 85),
(2, 'ME202', 'F2013', 40),
(3, 'EE213', 'S2014', 10),
(3, 'EE201', 'S2014', 90),
(4, 'EE213', 'F2013', 52),
(4, 'CENG230', 'F2015', 55),
(5, 'ME202', 'F2013', 49),
(5, 'CENG230', 'S2014', 53),
(6, 'EE213', 'F2014', 78),
(4, 'EE201', 'F2013', 50);

INSERT INTO Teaching (CourseId, Semester, ProfessorId) VALUES
('CENG100', 'F2013', 1),
('EE201', 'S2014', 2),
('CENG230', 'F2015', 2),
('CENG230', 'S2014', 2),
('ME202', 'F2013', 4),
('METE201', 'F2014', 5),
('CENG230', 'F2013', 5),
('EE213', 'F2013', 2),
('EE213', 'F2014', 2),
('EE213', 'S2014', 6);



-- Q1
select distinct Pro.Name
From Professor as Pro
where Pro.DeptId <> "CENG" and Exists (select 1
									   from Course as C, Teaching as T
                                       where C.DeptID = "CENG" and 
                                       C.CourseId = T.CourseId and
                                       Pro.ProfessorId = T.ProfessorId)
order by Pro.Name asc;




-- Q2 
select stu.Name
from Student as stu
where stu.StudentId in (select stu2.StudentId
						from Student as stu2, Transcript as T, Course as C
                        where T.StudentId = stu2.StudentId and C.CourseId = T.CourseId and C.CourseName = "Electrical Circuits Laboratory") and 
	  stu.StudentId in (select stu2.StudentId
						from Student as stu2, Transcript as T, Course as C
                        where T.StudentId = stu2.StudentId and C.CourseId = T.CourseId and C.CourseName = "Introduction to C Programming")
order by stu.Name asc;


-- Q2 can also be 
select stu.Name
from Student as stu
where stu.StudentId in (select stu2.StudentId
						from Student as stu2, Transcript as T, Course as C
                        where T.StudentId = stu2.StudentId and C.CourseId = T.CourseId and C.CourseName = "Electrical Circuits Laboratory") and
						-- not supported here INTERSECT
	  stu.StudentId in (select stu3.StudentId
						from Student as stu3, Transcript as T2, Course as C2
                        where T2.StudentId = stu3.StudentId and C2.CourseId = T2.CourseId and C2.CourseName = "Introduction to C Programming")
order by stu.Name asc;



-- Q3
select prof.Name, prof.Age
from Professor as prof 
where prof.DeptID = "CENG" or exists (select 1
									  from Teaching as T, Course as C
                                      where C.DeptID = "CENG" and T.CourseId = C.CourseId and prof.ProfessorId = T.ProfessorId)
order by prof.Name asc;
								
-- Q4
select stu.Name, stu.Gpa
from Student as stu
where stu.Gpa > (select max(stu2.Gpa)
				 from Student as stu2
                 where stu2.Status = "Senior");
                 


-- Q5
select distinct stu.StudentId, AVG(T.Score)
from Student as stu, Transcript as T
where stu.StudentId = T.StudentId and (T.Semester = "F2013" or T.Semester = "S2014") 
group by stu.StudentId
having AVG(T.Score) > 50
order by stu.StudentId asc;


-- Q6
select stu.Name
from Student as stu
where not exists (select 1
				  from Course as C
                  where C.DeptID = "EE" and 
				  not exists(select 1
							 from Transcript as T
							 where T.StudentId = stu.StudentId and 
							 T.courseId = C.CourseId))
order by stu.Name asc;


-- Q6 can also be written with Except
select stu.Name
from Student as stu
where not exists (select C.CourseId
				  from Course as C
                  where C.DeptID = "EE" 
                  except 
                  select T.CourseId
				  from Transkript as T
                  where T.StudentId = stu.StudentId)
order by stu.Name asc;
