drop schema if exists election cascade;
create schema election;
set search_path to election;

--could not enforce the email attribute of Volunteers, Staffs and Candidates
--tables are not overlapping, even though in people table it enforced a people
--can either be a candidate, volunteer or worker but not both.

--extra: enforced each donor can only donate once.
--extra: enforced all moderators are staff.
--extra: enforced one day can only have one debate.

--assume each campaign only has one candidate, and each candidate has only 
--one campaign
--assume a worker can only work for one campaign on one day with one role
--assume every debate takes 1 hour from the starting time
--assume donors are not workers or candidates

--A collection of all campaigns in the election,
--each campaign has unique cID and a spending limit.
create table Campaigns(
	cID integer primary key,
	spending_limit float not null
	);

--A collection of all people in campaigns, include volunteer, staff and candidate.
--Each people has a unique email, a name and a role,
--Each people can only have one role.
create table People(
	email varchar(50) primary key,
	name varchar(50) not null,
	type varchar(10) not null check (type in ('volunteer', 'staff', 'candidate'))
	);
	
--A collection of candidates, each candidate has a unique email, a name,
--and the cID of campaign he/she is participating
create table Candidates(
	c_email varchar(50) references People(email),
	c_name varchar(50) not null,
	cID integer not null references Campaigns(cID),
	primary key (c_email),
	unique(cID)
	);

--A collection of workers, each worker has a unique email, a name.
create table Workers(
	w_email varchar(50) references People(email),
	w_name varchar(50) not null,
	primary key (w_email)
	);

--A collection of records of the activities that workers have been worked on
--Every record has a worker email, a cID of the campaign the activity belongs to,
--a type of the activity, a start time and end time.
create table WorkersActivity(
	w_email varchar(50) not null references Workers(w_email),
	cID integer not null references campaigns(cID),
	activity_type varchar(50) not null,
	start_time timestamp not null,
	end_time timestamp not null,
	primary key (w_email, cID),
	check (end_time > start_time),
	check (activity_type in ('phone bank', 'door-to-door'))
	);

--A collection of volunteers, all volunteers are workers and have unique email
create table Volunteers(
	v_email varchar(50) references Workers(w_email),
	v_name varchar(50) not null,
	primary key (v_email)
	);

--A collection of staffs, all staffs are workers and have unique email
create table Staffs(
	s_email varchar(50) references Workers(w_email),
	s_name varchar(50) not null,
	primary key (s_email)
	);

--A collection of donors, each donor have unique email and have a address.
--Each donor make certain amount of donations to one campaign identified by cID 
--and in types of organization or individual.
create table Donors(
	d_email varchar(50) primary key,
	dornor_name varchar(50) not null,
	address varchar(50) not null,
	amount float not null,
	type varchar(50) not null check (type in ('organization', 'individual')),
	cID integer references Campaigns(cID)
	);

--A collection of debates, each debate has a unique dID and has a start time,
--each debate has one moderator which all moderator are staffs	
create table Debates(
	dID integer primary key,
	date_time timestamp not null,
	moderator_email varchar(50) not null references Staffs(s_email),
	unique (date_time, moderator_email),
	unique (date_time)
	);

--A collection of candidates in different debates.
--Candidates has unique email and participate in debate identified by dID.
create table DebateCandidates(
	dID integer not null references Debates(dID),
	c_email varchar(50) not null references Candidates(c_email),
	primary key (dID, c_email)
	);	
