--Query 1
REVOKE ALL ON Campaigns, People, Candidates, Workers, WorkersActivity, Donors,
WorkersActivity, Volunteers, Staffs, Debates, DebateCandidates 
FROM wangc471, guoyili2;
GRANT SELECT(cID) ON Campaigns TO wangc471, guoyili2;
GRANT SELECT(cID, type, amount) ON Donors TO wangc471, guoyili2;

--find all organization donations information
CREATE TEMP VIEW organization_donation AS
SELECT c.cID, 
       GREATEST(0,SUM(d.amount)) as total_organization_donations
FROM Campaigns c
LEFT JOIN Donors d ON c.cID = d.cID and d.type = 'organization'
GROUP BY c.cID
ORDER BY c.cID;

--find all individual donations information
CREATE TEMP VIEW individual_donation AS
SELECT c.cID, 
       GREATEST(0,SUM(d.amount)) as total_individual_donations
FROM Campaigns c
LEFT JOIN Donors d ON c.cID = d.cID and d.type = 'individual'
GROUP BY c.cID
ORDER BY c.cID;

--answer
SELECT * 
FROM organization_donation
NATURAL JOIN individual_donation;

DROP VIEW individual_donation, organization_donation;


--Query 2
REVOKE ALL ON Campaigns, People, Candidates, Workers, WorkersActivity, Donors,
WorkersActivity, Volunteers, Staffs, Debates, DebateCandidates 
FROM wangc471, guoyili2;
GRANT SELECT(cID) ON Campaigns TO wangc471, guoyili2;
GRANT SELECT(w_email, cID) ON WorkersActivity TO wangc471, guoyili2;
GRANT SELECT(v_email, v_name) ON Volunteers TO wangc471, guoyili2;
SELECT v.v_email
FROM Volunteers v
--filter the email that do not attend every campaign
WHERE NOT EXISTS(
    SELECT c.cID
    FROM Campaigns c
    --show the non exist volunteer email and cID after matching worker email to volunteer email  
    -- and the work activity cid to the campaign cid 
    WHERE NOT EXISTS(
        SELECT wa.cID
        FROM WorkersActivity wa
        WHERE wa.w_email = v.v_email AND wa.cID = c.cID
    )
);


--Query 3
REVOKE ALL ON Campaigns, People, Candidates, Workers, WorkersActivity, Donors,
WorkersActivity, Volunteers, Staffs, Debates, DebateCandidates 
FROM wangc471, guoyili2;
GRANT SELECT(c_email, c_name) ON Candidates TO wangc471, guoyili2;
GRANT SELECT(dID) ON Debates TO wangc471, guoyili2;
GRANT SELECT(dID, c_email) ON DebateCandidates TO wangc471, guoyili2;

SELECT c.c_email
FROM Candidates c
--filter the email that do not attend every debate
WHERE NOT EXISTS(
    SELECT d.dID
    FROM Debates d
    --show the non exist candidate email and dID after matching debate email to candidate email  
    -- and the debatecandidate did  to the debate did 
    WHERE NOT EXISTS(
        SELECT dc.dID
        FROM DebateCandidates dc
        WHERE dc.c_email = c.c_email AND d.dID = dc.dID
    )
);

