"""CSC343 Assignment 2

=== CSC343 Winter 2023 ===
Department of Computer Science,
University of Toronto

This code is provided solely for the personal and private use of
students taking the CSC343 course at the University of Toronto.
Copying for purposes other than this use is expressly prohibited.
All forms of distribution of this code, whether as given or with
any changes, are expressly prohibited.

Authors: Danny Heap, Marina Tawfik, and Jacqueline Smith

All of the files in this directory and all subdirectories are:
Copyright (c) 2023 Danny Heap and Jacqueline Smith

=== Module Description ===

This file contains the WasteWrangler class and some simple testing functions.
"""

import datetime as dt
import psycopg2 as pg
import psycopg2.extensions as pg_ext
import psycopg2.extras as pg_extras
from typing import Optional, TextIO


class WasteWrangler:
    """A class that can work with data conforming to the schema in
    waste_wrangler_schema.ddl.

    === Instance Attributes ===
    connection: connection to a PostgreSQL database of a waste management
    service.

    Representation invariants:
    - The database to which connection is established conforms to the schema
      in waste_wrangler_schema.ddl.
    """
    connection: Optional[pg_ext.connection]

    def __init__(self) -> None:
        """Initialize this WasteWrangler instance, with no database connection
        yet.
        """
        self.connection = None

    def connect(self, dbname: str, username: str, password: str) -> bool:
        """Establish a connection to the database <dbname> using the
        username <username> and password <password>, and assign it to the
        instance attribute <connection>. In addition, set the search path
        to waste_wrangler.

        Return True if the connection was made successfully, False otherwise.
        I.e., do NOT throw an error if making the connection fails.

        >>> ww = WasteWrangler()
        >>> ww.connect("csc343h-marinat", "marinat", "")
        True
        >>> # In this example, the connection cannot be made.
        >>> ww.connect("invalid", "nonsense", "incorrect")
        False
        """
        try:
            self.connection = pg.connect(
                dbname=dbname, user=username, password=password,
                options="-c search_path=waste_wrangler"
            )
            return True
        except pg.Error:
            return False

    def disconnect(self) -> bool:
        """Close this WasteWrangler's connection to the database.

        Return True if closing the connection was successful, False otherwise.
        I.e., do NOT throw an error if closing the connection failed.

        >>> ww = WasteWrangler()
        >>> ww.connect("csc343h-marinat", "marinat", "")
        True
        >>> ww.disconnect()
        True
        """
        try:
            if self.connection and not self.connection.closed:
                self.connection.close()
            return True
        except pg.Error:
            return False


    def schedule_trip(self, rid: int, time: dt.datetime) -> bool:
        """Schedule a truck and two employees to the route identified
        with <rid> at the given time stamp <time> to pick up an
        unknown volume of waste, and deliver it to the appropriate facility.

        The employees and truck selected for this trip must be available:
            * They can NOT be scheduled for a different trip from 30 minutes
              of the expected start until 30 minutes after the end time of this
              trip.
            * The truck can NOT be scheduled for maintenance on the same day.

        The end time of a trip can be computed by assuming that all trucks
        travel at an average of 5 kph.

        From the available trucks, pick a truck that can carry the same
        waste type as <rid> and give priority based on larger capacity and
        use the ascending order of ids to break ties.

        From the available employees, give preference based on hireDate
        (employees who have the most experience get priority), and order by
        ascending order of ids in case of ties, such that at least one
        employee can drive the truck type of the selected truck.

        Pick a facility that has the same waste type a <rid> and select the one
        with the lowest fID.

        Return True iff a trip has been scheduled successfully for the given
            route.
        This method should NOT throw an error i.e. if scheduling fails, the
        method should simply return False.

        No changes should be made to the database if scheduling the trip fails.

        Scheduling fails i.e., the method returns False, if any of the following
        is true:
            * If rid is an invalid route ID.
            * If no appropriate truck, drivers or facility can be found.
            * If a trip has already been scheduled for <rid> on the same day
              as <time> (that encompasses the exact same time as <time>).
            * If the trip can't be scheduled within working hours i.e., between
              8:00-16:00.

        While a realistic use case will provide a <time> in the near future, our
        tests could use any valid value for <time>.
        """
        try:
            # TODO: implement this method
            cur = self.connection.cursor()
            print("hello!!!!!")
            print(time.date())
            print(time.time())
          
            work_start = dt.datetime(1, 1, 1, 8, 0)
            work_end = dt.datetime(1, 1, 1, 16, 0)
            
            break_time = dt.timedelta(hours = 0.5)        
            
            #schedule_time = dt.datetime(2023, 5, 4, 7, 0) #=time
            schedule_time = time
            
            cur.execute("select length from Route where rID = %s",(rid,))
            # if rid not exist, return false
            if cur.rowcount == 0:
            	print("no matched rid")
            	return False;
            	
            for row in cur:
            	print(row[0])
            	#trip should be done before work_end time
            	work_end = work_end - dt.timedelta(hours = row[0]/5)
            	trip_begin = time - break_time
            	trip_end = time + dt.timedelta(hours = row[0]/5) + break_time
            	print("trip start:",trip_begin, "trip end:", trip_end)
            	print(work_end)
            	
            #check is the schedule time in working hour
            if (schedule_time.time() < work_start.time() or schedule_time.time() > work_end.time()):
            	print("not in working hour!")
            	return False;
            else: 
            	print("good time")
            
            next_day = time.date() + dt.timedelta(days=1)
            print(next_day)
            print(time.date())
	    
	    #check is the schedule day has a trip on this route already
            cur.execute("select * from Trip where tTIME > %s and tTIME < %s", (time.date(), next_day,))
            for row in cur:
            	if (row[0] == rid):
            	    print("ha?")
            	    return False;
            	else:
            	    print("here")
            	    print((rid,))
            
            #find all drivers and truck pairs on the schedule day as <drivers_trucks>
            cur.execute("create temporary view drivers_trucks as select rID, tID, eID1, eID2, tTIME from Trip where tTIME > %s and tTIME < %s", (time.date(), next_day,))  
            
            cur.execute("select * from drivers_trucks")
            for row in cur:
            	print(row)
            
            #comupte the end_time for every trip on that day based on drivers_trucks as <trip_end_start>
            cur.execute("create temporary view trip_end_start as select rID, tID, eID1, eID2, tTIME, tTIME + interval '1 second' * (3600*length/5) as end_time from drivers_trucks natural join Route")
            
            print("duration: ")
            cur.execute("select * from trip_end_start")
            for row in cur:
            	print(row)  	
            
            #compute the conflicted trucks driver pairs as <conflict_drivers_trucks>
            cur.execute("create temporary view conflict_drivers_trucks as select tID, eID1, eID2 from trip_end_start where (tTIME, end_time) OVERLAPS (%s, %s)", (trip_begin, trip_end,))
            
            cur.execute("select * from conflict_drivers_trucks")
            print("conflict:")
            for row in cur:
            	print(row)
            	
            #find maintained trucks on the day
            cur.execute("create temporary view maintained_truck as select tID from Maintenance where mDATE = %s", (time.date(),))
            cur.execute("select * from maintained_truck")
            print("maintained trucks:")
            for row in cur:
            	print(row)
            
            #find type matched trucks
            cur.execute("create temporary view matched_truck as select tID, capacity, truckType from Truck natural join TruckType natural join Route where rID = %s", (rid,))
            cur.execute("select * from matched_truck")
            print("matched trucks:")
            for row in cur:
            	print(row)
            	
            #find avaliable trucks
            cur.execute("create temporary view valid_truck as (((select tID from matched_truck) except (select tID from conflict_drivers_trucks)) except (select tID from maintained_truck))")
           
            cur.execute("select * from valid_truck")
            print("valid trucks:")
            for row in cur:
            	print(row)
            
            #find the final truck
            cur.execute("create temporary view selected_truck as select * from valid_truck natural join matched_truck") 
            cur.execute("select * from selected_truck order by capacity desc, tID")
            print("select trucks:")
            if cur.rowcount == 0:
            	print("no matched rid")
            	return False;
            for row in cur:
            	print(row)
            
            #find all capable drivers avaliable with no conflicts with other trips
            cur.execute("create temporary view capable_drivers as select * from employee natural join driver where not exists (select * from conflict_drivers_trucks where employee.eID = eID1 or employee.eID = eID2) order by hireDate, eID")
            cur.execute("select * from capable_drivers")
            print("capable drivers:")
            for row in cur:
            	print(row)
            	
            #select all drivers that is able to drive types of truck that are avaliable for this trip
            cur.execute("create temporary view matched_drivers as select eID, name, hireDate, capacity, tID, truckType from capable_drivers natural join selected_truck order by capacity desc, tID, hireDate, eID")
           
            #find the first driver who able to drive this truckType
            cur.execute("select * from matched_drivers")
            print("first drivers: ")
            first_truck = cur.fetchone()
            if (first_truck == None):
            	print("no first driver")
            	return False;
            print(first_truck)
  
            #select the driver with earliest hiredate 
            cur.execute("select * from capable_drivers where eID != %s", (first_truck[0],))
            print("second drivers: ")
            second_truck = cur.fetchone()
            if (second_truck == None):
            	print("no second driver")
            	return False;
            print(second_truck)
            
            #find matching facilities
            cur.execute("select fID from Facility natural join Route where rID = %s order by fID", (rid,))
            selected_fid = cur.fetchone()
            if (selected_fid == None):
            	print("no matches facility")
            	return False;
            print(selected_fid)
            
            if (second_truck[0] > first_truck[0]):
            	temp = first_truck
            	first_truck = second_truck
            	second_truck = temp
            	truck_tid = second_truck[4]
            print(first_truck)
            print(second_truck)	
            
            value = 'NULL'
            cur.execute("insert into Trip values (%s, %s, '%s', %s, %s, %s, %s);" %(rid, truck_tid, time, value, first_truck[0], second_truck[0], selected_fid[0],))
            #print("hi")
            cur.execute("select * from Trip")
            for row in cur:
            	print(row)
            
            self.connection.commit()
            cur.close()
            
            	    
            return True;
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            return False

    def schedule_trips(self, tid: int, date: dt.date) -> int:
        """Schedule the truck identified with <tid> for trips on <date> using
        the following approach:

            1. Find routes not already scheduled for <date>, for which <tid>
               is able to carry the waste type. Schedule these by ascending
               order of rIDs.

            2. Starting from 8 a.m., find the earliest available pair
               of drivers who are available all day. Give preference
               based on hireDate (employees who have the most
               experience get priority), and break ties by choosing
               the lower eID, such that at least one employee can
               drive the truck type of <tid>.

               The facility for the trip is the one with the lowest fID that can
               handle the waste type of the route.

               The volume for the scheduled trip should be null.

            3. Continue scheduling, making sure to leave 30 minutes between
               the end of one trip and the start of the next, using the
               assumption that <tid> will travel an average of 5 kph.
               Make sure that the last trip will not end after 4 p.m.

        Return the number of trips that were scheduled successfully.

        Your method should NOT raise an error.

        While a realistic use case will provide a <date> in the near future, our
        tests could use any valid value for <date>.
        """
        # TODO: implement this method
        cur = self.connection.cursor()        
        #find avaliable driver, hiredate pairs
        cur.execute("create temporary view avaliable_driver_info as select eID, truckType, hireDate from (select eID from Driver except ((select eID1 as eID from Trip where tTIME > %s and tTime < %s) union (select eID2 as eID from Trip where tTIME > %s and tTime < %s))) as t3 natural join Driver natural join Employee order by hireDate, eID", (date, date + dt.timedelta(days=1), date, date + dt.timedelta(days=1),))
        #cur.execute
        cur.execute("select * from avaliable_driver_info")
        eid = []
       
        for row in cur:
            if row[0] not in eid:
                eid.append(row[0])
                    
        #find avaliable trucks
        cur.execute("create temporary view avaliable_truck as select tID from Truck except (select tID from Trip where tTIME > %s and tTIME < %s)", (date, date + dt.timedelta(days=1),))
        cur.execute("select tID from avaliable_truck")
        tid = []
        for row in cur:
            tid.append(row[0])
            
        
        #find avaliable truck info
        cur.execute("create temporary view all_possible as select rID, tID, eID, hireDate truckType, wasteType, fID, length from (select tID, truckType, wasteType from avaliable_truck natural join Truck natural join TruckType ) as t1 natural join (select rID, length, wasteType from (select rID from Route except (select rID from Trip where tTIME > %s and tTime < %s) order by rID) as t3 natural join route) as t5 natural join avaliable_driver_info natural join Facility order by rID, tID, hireDate, eID, fID", (date, date + dt.timedelta(days=1),))
        cur.execute("select * from all_possible")
        
        all_possible = []
        for row in cur:
            all_possible.append(row)            
        work_start = dt.datetime(1, 1, 1, 8, 0)
        work_end = dt.datetime(1, 1, 1, 16, 0)
        break_time = dt.timedelta(hours = 0.5)   
        current_time = work_start
        
        conflict_eid = []
        conflict_tid = []
        conflict_rid = []
        num_of_trip = 0
        while len(all_possible) > 0:
            first = all_possible[0]
            if (current_time + dt.timedelta(hours = first[-1]/5)).time() > work_end.time():
                all_possible.pop(all_possible.index(first))
                continue
            
            eid.pop(eid.index(first[2]))
            second_eid = eid[0]
            #if second_eid 
            eid.pop(eid.index(second_eid))
            
            current_time += dt.timedelta(hours = first[-1]/5) + break_time
            num_of_trip += 1
            conflict_rid.append(first[0])
            conflict_tid.append(first[1])
            conflict_eid.append(first[2])
            conflict_eid.append(second_eid)
            for i in all_possible[:]:
                if i[0] in conflict_rid or i[1] in conflict_tid or i[2] in conflict_eid:
                    all_possible.pop(all_possible.index(i))

        return num_of_trip  

    def update_technicians(self, qualifications_file: TextIO) -> int:
        """Given the open file <qualifications_file> that follows the format
        described on the handout, update the database to reflect that the
        recorded technicians can now work on the corresponding given truck type.

        For the purposes of this method, you may assume that no two employees
        in our database have the same name i.e., an employee can be uniquely
        identified using their name.

        Your method should NOT throw an error.
        Instead, only correct entries should be reflected in the database.
        Return the number of successful changes, which is the same as the number
        of valid entries.
        Invalid entries include:
            * Incorrect employee name.
            * Incorrect truck type.
            * The technician is already recorded to work on the corresponding
              truck type.
            * The employee is a driver.

        Hint: We have provided a helper _read_qualifications_file that you
            might find helpful for completing this method.
        """
        try:
            # TODO: implement this method
            cur = self.connection.cursor()
            employees = self._read_qualifications_file(qualifications_file) 
            technicians = []
            eids = []
            truck_types = []
            for i in employees:
                print(i)
                fullname = i[0]+ " " + i[1]
                print("fullname: ")
                print(fullname)
                #check is this person is an employee
                cur.execute("select eID from Employee where name = %s", (fullname,))
                if cur.rowcount == 0:
                    print("no matched eid")
                    continue
                for row in cur:
                    print(row)
                    eid = row[0]
                cur.execute("select eID from Driver where eID = %s", (eid,))
                if cur.rowcount == 0:
                    print("not a driver")
                else:
                    print("oop! driver cannot be tech!")
                    continue
                #get technician list need to be insert
                if eid not in eids:
                    eids.append(eid)
                #get corresponding technician eid list
                
                if fullname not in technicians:
                    technicians.append(fullname)
                
                idx = eids.index(eid)
                if len(truck_types) < len(eids):
                    truck_types.append([])
                print(idx)
                
                existed_truckType = []
                cur.execute("select eID, truckType from Technician where eid = %s", (eid,))
                print("tech pairs1")
                for row in cur:
                    print(row)
                    existed_truckType.append(row[1])
                
                cur.execute("select truckType from TruckType where truckType = %s", (i[2],))
                if cur.rowcount == 0:
                    print("not valid truckType:")
                    print(i[2])
                    continue
                if i[2] not in truck_types[idx] and i[2] not in existed_truckType:
                    truck_types[idx].append(i[2])
                    
                print("exist type:")
                for tp in existed_truckType:
                    print(tp)
                print("tech:")
                for tech in technicians:
                    print(tech)
                print("id:")
                for id in eids:
                    print(id)
                print("types:")
                for tp in truck_types:
                    print(tp)
                    print("end\n")

            count = 0
            for i in range(len(technicians)):
            	for j in truck_types[i]:
                    cur.execute("insert into Technician values (%s, '%s');" %(eids[i], j,))
                    count += 1
            	#print("hi")
               
            print("count:", count)
            self.connection.commit()
            cur.close()       
            return count             	            

        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            return 0



 
     
    def workmate_sphere(self, eid: int) -> list[int]:
                       
        """Return the workmate sphere of the driver identified by <eid>, as a
        list of eIDs.

        The workmate sphere of <eid> is:
            * Any employee who has been on a trip with <eid>.
            * Recursively, any employee who has been on a trip with an employee
              in <eid>'s workmate sphere is also in <eid>'s workmate sphere.

        The returned list should NOT include <eid> and should NOT include
        duplicates.

        The order of the returned ids does NOT matter.

        Your method should NOT return an error. If an error occurs, your method
        should simply return an empty list.
        """
      
        try:
            def getnb_1(info,result,eid):
                for row in info:
                   if row[0]==eid and row[1] not in result :
                       result.add(row[1])
                       getnb_2(info,result,row[1])
                       getnb_1(info,result,row[1])

            def getnb_2(info,result,eid):
                for row in info:
                   if row[1]==eid and row[0] not in result :
                       result.add(row[0])
                       getnb_1(info,result,row[0])               
                       getnb_2(info,result,row[0])          
            # TODO: implement this method
            #open cursor
            cursor1 = self.connection.cursor()           
            cursor1.execute("SELECT eid1,eid2 FROM trip")                           
            trips=[]                            
            for row in cursor1.fetchall():
                trips.append(row)               
             
            result={eid}
            getnb_1(trips,result,eid)
            getnb_2(trips,result,eid)
            result.remove(eid)
            result=list(result)    
            return result
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            return []

    def schedule_maintenance(self, date: dt.date) -> int:
        """For each truck whose most recent maintenance before <date> happened
        over 90 days before <date>, and for which there is no scheduled
        maintenance up to 10 days following date, schedule maintenance with
        a technician qualified to work on that truck in ascending order of tIDs.

        For example, if <date> is 2023-05-02, then you should consider trucks
        that had maintenance before 2023-02-01, and for which there is no
        scheduled maintenance from 2023-05-02 to 2023-05-12 inclusive.

        Choose the first day after <date> when there is a qualified technician
        available (not scheduled to maintain another truck that day) and the
        truck is not scheduled for a trip or maintenance on that day.

        If there is more than one technician available on a given day, choose
        the one with the lowest eID.

        Return the number of trucks that were successfully scheduled for
        maintenance.

        Your method should NOT throw an error.

        While a realistic use case will provide a <date> in the near future, our
        tests could use any valid value for <date>.
        """
        try:
            # TODO: implement this method
            cur = self.connection.cursor()
            #find the limit maintain dates
            maintain_dates = date - dt.timedelta(days=90)
            schedule_dates = date + dt.timedelta(days=10)
            print("maintain date:", maintain_dates)
            print("schedule date:", schedule_dates)
            #find all trucks that has a maintenance within 90 days
            cur.execute("create temporary view maintained_in_days as select * from Maintenance where mDate > %s and mDate < %s", (maintain_dates, date,))
            cur.execute("select * from maintained_in_days")
            print("have maintain trucks:")
            for row in cur:
            	print(row)
            	
            #find all trucks does have scheduled maintenance in 10 days
            cur.execute("create temporary view schedule_in_days as select * from Maintenance where mDate <= %s and mDate >= %s", (schedule_dates, date,))
            cur.execute("select * from schedule_in_days")
            print("have scheduel trucks:")
            for row in cur:
            	print(row)
            		
            #find all trucks need to be scheduled	
            cur.execute("create temporary view not_maintain_trucks as ((select tID from Truck) except (select tID from maintained_in_days) except (select tID from schedule_in_days)) ")
            cur.execute("select * from not_maintain_trucks")
            print("need maintain trucks:")
            for row in cur:
            	print(row)
            
            #compute need maintained truck-type pairs
            num_of_schedule = 0
            cur.execute("create temporary view main_truck_type_pair as select tID, truckType from not_maintain_trucks natural join Truck")
            cur.execute("select * from main_truck_type_pair")
            print("need maintain trucks pairs:")
            for row in cur:
            	print(row)
            	num_of_schedule += 1
            print("number of schedules:",num_of_schedule)
            	
            #find future maintenance plan
            cur.execute("create temporary view future_maintain as select tID, eID, mDate from Maintenance where mDate > %s", (date,))
            cur.execute("select * from future_maintain")
            print("future maintain plan:")
            for row in cur:
            	print(row)
            
            #create all possible maintain paris (tID, eID)
            cur.execute("create temporary view possible_maintain as select tID, eID, truckType from main_truck_type_pair natural join Technician order by tID, eID")
            cur.execute("select * from possible_maintain")
            print("all possible:")
            for row in cur:
            	print(row)
            
            cur.execute("select count(distinct tID) from possible_maintain")
            print("numbers of scheduable maintenance:")
            for row in cur:
            	num_of_schedule = row[0]
            print(num_of_schedule)
            if num_of_schedule == 0:
            	print("no valid technician")
            	return 0;
                 
            	
            current_date = date
            schedules = []
            schedules_eid = []
            schedules_tid = []
           
            while (num_of_schedule != len(schedules)):
            
            	conflict_tid = []
            	conflict_eid = []
            	all_possible = []
            	print("schedule length:", len(schedules))
            	
            	current_date = current_date + dt.timedelta(days=1)
            	print(current_date)
            	
            	cur.execute("select tID from Trip where tTIME > %s and tTIME < %s union select tID from Maintenance where mDate = %s", (current_date, current_date + dt.timedelta(days=1), current_date,))
            	for row in cur:           	    
            	    conflict_tid.append(row[0])
            	for i in conflict_tid:
            	    print("conflict_tid:", i) 
            	
            	cur.execute("select eID from Maintenance where mDate = %s", (current_date,))
            	for row in cur:           	    
            	    conflict_eid.append(row[0])
            	for i in conflict_eid:
            	    print("conflict_eid:", i) 
            	    
            	cur.execute("select * from possible_maintain")
            	
            	print("all schedules eid:",schedules_eid)
            	print("all schedules tid:",schedules_tid)
            	
            	
            	for row in cur:
            	    if (row[0] not in conflict_tid and row[1] not in conflict_eid and row[0] and row[0] not in schedules_tid):
            	        all_possible.append(row)
            	    
            	print("all possible:")
            	for i in all_possible:
            	    print(i)
            	
            	while len(all_possible) > 0:
            	    first = all_possible[0]
            	    schedules.append((first[0], first[1], current_date))
            	    schedules_tid.append(first[0])
            	    schedules_eid.append(first[1])
            	    for i in all_possible[:]:
            	        #print("i:",i)
            	        #print("first",first)
            	        if i[0] == first[0] or i[1] == first[1]:
            	            #print("removed!")
            	            all_possible.pop(all_possible.index(i))
            	    
            	    #print("all possible:",all_possible)
            	print("all schedules:",schedules)
            	print("all schedules eid:",schedules_eid)
            	print("all schedules tid:",schedules_tid)
            
            print("final schedule length:", len(schedules))
            
            for i in schedules:
            	cur.execute("insert into Maintenance values (%s, %s, '%s');" %(i[0], i[1], i[2],))
            	#print("hi")
            	
            self.connection.commit()
            cur.close() 
            
            return len(schedules)
            	    
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            return 0

    def reroute_waste(self, fid: int, date: dt.date) -> int:
        """Reroute the trips to <fid> on day <date> to another facility that
        takes the same type of waste. If there are many such facilities, pick
        the one with the smallest fID (that is not <fid>).

        Return the number of re-routed trips.

        Don't worry about too many trips arriving at the same time to the same
        facility. Each facility has ample receiving facility.

        Your method should NOT return an error. If an error occurs, your method
        should simply return 0 i.e., no trips have been re-routed.

        While a realistic use case will provide a <date> in the near future, our
        tests could use any valid value for <date>.

        Assume this happens before any of the trips have reached <fid>.
        """
        try:
            cursor1 = self.connection.cursor()
            cursor1.execute("UPDATE trip SET fid=(SELECT MIN(fID) minfid FROM facility WHERE wasteType=(SELECT wasteType FROM facility WHERE fid=%s) AND fid<>%s)  WHERE fid=%s and ttime::date = %s::date",[fid,fid,fid,date])
            self.connection.commit()
            return cursor1.rowcount
            
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            return 0

    # =========================== Helper methods ============================= #

    @staticmethod
    def _read_qualifications_file(file: TextIO) -> list[list[str, str, str]]:
        """Helper for update_technicians. Accept an open file <file> that
        follows the format described on the A2 handout and return a list
        representing the information in the file, where each item in the list
        includes the following 3 elements in this order:
            * The first name of the technician.
            * The last name of the technician.
            * The truck type that the technician is currently qualified to work
              on.

        Pre-condition:
            <file> follows the format given on the A2 handout.
        """
        result = []
        employee_info = []
        for idx, line in enumerate(file):
            if idx % 2 == 0:
                info = line.strip().split(' ')[-2:]
                fname, lname = info
                employee_info.extend([fname, lname])
            else:
                employee_info.append(line.strip())
                result.append(employee_info)
                employee_info = []

        return result


def setup(dbname: str, username: str, password: str, file_path: str) -> None:
    """Set up the testing environment for the database <dbname> using the
    username <username> and password <password> by importing the schema file
    and the file containing the data at <file_path>.
    """
    connection, cursor, schema_file, data_file = None, None, None, None
    try:
        # Change this to connect to your own database
        connection = pg.connect(
            dbname=dbname, user=username, password=password,
            options="-c search_path=waste_wrangler"
        )
        cursor = connection.cursor()

        schema_file = open("./waste_wrangler_schema.sql", "r")
        cursor.execute(schema_file.read())

        data_file = open(file_path, "r")
        cursor.execute(data_file.read())

        connection.commit()
    except Exception as ex:
        connection.rollback()
        raise Exception(f"Couldn't set up environment for tests: \n{ex}")
    finally:
        if cursor and not cursor.closed:
            cursor.close()
        if connection and not connection.closed:
            connection.close()
        if schema_file:
            schema_file.close()
        if data_file:
            data_file.close()


def test_preliminary() -> None:
    """Test preliminary aspects of the A2 methods."""
    ww = WasteWrangler()
    qf = None
    try:
        # TODO: Change the values of the following variables to connect to your
        #  own database:
        dbname = 'csc343h-wangc471'
        user = 'wangc471'
        password = 'Cnshuimu0308'

        connected = ww.connect(dbname, user, password)

        # The following is an assert statement. It checks that the value for
        # connected is True. The message after the comma will be printed if
        # that is not the case (connected is False).
        # Use the same notation to thoroughly test the methods we have provided
        assert connected, f"[Connected] Expected True | Got {connected}."

        # TODO: Test one or more methods here, or better yet, make more testing
        #   functions, with each testing a different aspect of the code.

        # The following function will set up the testing environment by loading
        # the sample data we have provided into your database. You can create
        # more sample data files and use the same function to load them into
        # your database.
        # Note: make sure that the schema and data files are in the same
        # directory (folder) as your a2.py file.
        setup(dbname, user, password, './waste_wrangler_data.sql')

        # --------------------- Testing schedule_trip  ------------------------#

        # You will need to check that data in the Trip relation has been
        # changed accordingly. The following row would now be added:
        # (1, 1, '2023-05-04 08:00', null, 2, 1, 1)
        
        scheduled_trip = ww.schedule_trip(1, dt.datetime(2023, 5, 4, 8, 0))
        assert scheduled_trip, \
            f"[Schedule Trip] Expected True, Got {scheduled_trip}"

        # Can't schedule the same route of the same day.
        scheduled_trip = ww.schedule_trip(1, dt.datetime(2023, 5, 4, 13, 0))
        assert not scheduled_trip, \
            f"[Schedule Trip] Expected False, Got {scheduled_trip}"

        # -------------------- Testing schedule_trips  ------------------------#

        # All routes for truck tid are scheduled on that day
        scheduled_trips = ww.schedule_trips(1, dt.datetime(2023, 5, 3))
        assert scheduled_trips == 0, \
            f"[Schedule Trips] Expected 0, Got {scheduled_trips}"

        # ----------------- Testing update_technicians  -----------------------#

        # This uses the provided file. We recommend you make up your custom
        # file to thoroughly test your implementation.
        # You will need to check that data in the Technician relation has been
        # changed accordingly
        #qf = open('qualifications.txt', 'r')
        #updated_technicians = ww.update_technicians(qf)
        #assert updated_technicians == 2, \
           # f"[Update Technicians] Expected 2, Got {updated_technicians}"

        # ----------------- Testing workmate_sphere ---------------------------#

        # This employee doesn't exist in our instance
        #workmate_sphere = ww.workmate_sphere(2023)
        #assert len(workmate_sphere) == 0, \
        #    f"[Workmate Sphere] Expected [], Got {workmate_sphere}"

        #workmate_sphere = ww.workmate_sphere(3)
         #Use set for comparing the results of workmate_sphere since
         #order doesn't matter.
         #Notice that 2 is added to 1's work sphere because of the trip we
         #added earlier.
        #assert set(workmate_sphere) == {1, 2}, \
        #    f"[Workmate Sphere] Expected {{1, 2}}, Got {workmate_sphere}"

        # ----------------- Testing schedule_maintenance ----------------------#

        # You will need to check the data in the Maintenance relation
        #scheduled_maintenance = ww.schedule_maintenance(dt.date(2023, 5, 5))
        #assert scheduled_maintenance == 7, \
            #f"[Schedule Maintenance] Expected 7, Got {scheduled_maintenance}"

        # ------------------ Testing reroute_waste  ---------------------------#

        # There is no trips to facility 1 on that day
        reroute_waste = ww.reroute_waste(1, dt.date(2023, 5, 10))
        assert reroute_waste == 0, \
            f"[Reroute Waste] Expected 0. Got {reroute_waste}"

        # You will need to check that data in the Trip relation has been
        # changed accordingly
        reroute_waste = ww.reroute_waste(1, dt.date(2023, 5, 3))
        assert reroute_waste == 1, \
            f"[Reroute Waste] Expected 1. Got {reroute_waste}"
    finally:
        if qf and not qf.closed:
            qf.close()
        ww.disconnect()


if __name__ == '__main__':
    # Un comment-out the next two lines if you would like to run the doctest
    # examples (see ">>>" in the methods connect and disconnect)
    # import doctest
    # doctest.testmod()

    # TODO: Put your testing code here, or call testing functions such as
    #   this one:
    test_preliminary()
