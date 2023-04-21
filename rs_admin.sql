-- General Admin

-- Show running queries
    SELECT user_name, db_name, pid, query
    FROM stv_recents
    WHERE status = 'Running';

-- Show recent queries (td, join with user table)
    SELECT userid,query,pid,substring,starttime,endtime,elapsed,aborted
    FROM svl_qlog 
    ORDER BY starttime DESC
    LIMIT 100; 

-- Show recent connections
    select recordtime, username, dbname, remotehost, remoteport
    from stl_connection_log
    where event = 'initiating session'
    and pid not in 
    (select pid from stl_connection_log
    where event = 'disconnecting session')
    order by 1 desc;

-- Move a table from one schema to another (remaps data, drop dependecies)
    CREATE TABLE pwtest.customer (LIKE public.customer);
    ALTER TABLE pwtest.customer APPEND FROM public.customer;
    DROP TABLE public.customer;
    
-- Users

-- Show current user
    SELECT current_user;

-- Show all Users
    SELECT * FROM pg_user;

-- Impersonate User (requires supeuser)
    SET SESSION AUTHORIZATION 'finance';
    RESET SESSION AUTHORIZATION;

-- Create/drop new user
    CREATE USER pete PASSWORD 'r4nd0mZ1KpasswUrd0';
    DROP USER pete;

-- Change a users password
    ALTER USER pete PASSWORD 'r4nd0mqeXpasswUrd0';

-- Create new temporary user (with expiry date) ++ with timestamp
    CREATE USER pete_expire PASSWORD 'r4nd0mdj9passwUrd0' VALID UNTIL '2020-05-19 06:15:00+00';

-- Create a new user that expires tomorrow
    CREATE USER pete_expire_tomorrow PASSWORD 'r4nd0mD0JpasswUrd0' VALID UNTIL sysdate+1;

-- Create new superuser
    CREATE USER pete CREATEUSER PASSWORD 'r4nd0mX8vpasswUrd0'

-- Amend user, give/remove superuser access
    ALTER USER pete CREATEUSER;
    ALTER USER pete NOCREATEUSER;

-- Groups

-- Show all Groups
    SELECT * 
    FROM pg_group;

-- Create a Group
    CREATE GROUP test_group WITH USER pete;

-- Create a Group and Add Users
    CREATE GROUP test_group WITH USER pete1, pete2;

-- Add a User to a Group
    ALTER GROUP test_group ADD USER pete1;

-- Amend a Group Name
    ALTER GROUP test_group RENAME TO group_test

-- Show Users & their accosiated Groups
    SELECT usename, groname 
    FROM pg_user, pg_group
    WHERE pg_user.usesysid = ANY(pg_group.grolist)
    AND pg_group.groname in (SELECT DISTINCT pg_group.groname from pg_group);
    
-- Create Tables/Views/MViews
    
-- Show all Schemas
    SELECT * FROM pg_namespace;

-- Create a Schema
    CREATE SCHEMA pwtest

-- Show all tables
    SELECT * FROM pg_table_def 

-- Create a Table
    CREATE TABLE pwtest.customer 
    (
    customerid       INT8 NOT NULL,
    customername     VARCHAR(25) NOT NULL,
    phone            CHAR(15) NOT NULL,
    nationid        INT4 NOT NULL,
    marketsegment    CHAR(10) NOT NULL,
    accountbalance   NUMERIC(12,2) NOT NULL
    );
    CREATE TABLE pwtest.nation 
    (
    nationid    INT4 NOT NULL,
    nationname   CHAR(25) NOT NULL
    );

-- Drop a table
    DROP TABLE pwtest.customer

-- Insert rows to Table
    INSERT INTO pwtest.customer VALUES
    (1, 'Customer#000000001', '33-687-542-7601', 3, 'HOUSEHOLD', 2788.52),
    (2, 'Customer#000000002', '13-806-545-9701', 1, 'MACHINERY', 591.98),
    (3, 'Customer#000000003', '13-312-472-8245', 1, 'HOUSEHOLD', 3332.02),
    (4, 'Customer#000000004', '23-127-851-8031', 2, 'MACHINERY', 9255.67),
    (5, 'Customer#000000005', '13-137-193-2709', 1, 'BUILDING', 5679.84)
    ;
    INSERT INTO pwtest.nation VALUES
    (1, 'UNITED STATES'),
    (2, 'AUSTRALIA'),
    (3, 'UNITED KINGDOM');

-- Updates to tables
    UPDATE customer 
    SET accountbalance = 1000 
    WHERE marketsegment = 'BUILDING';

-- Create View
    CREATE OR REPLACE VIEW pwtest.customer_vw 
        AS SELECT customername, phone, marketsegment, accountbalance, 
        CASE WHEN accountbalance < 1000 THEN 'low' WHEN accountbalance > 1000 AND accountbalance < 5000 THEN 'middle' 
        ELSE 'high' END AS incomegroup 
        FROM pwtest.customer;

-- Drop a View
    DROP VIEW pwtest.customer_vw

-- Create Materialized View
    CREATE MATERIALIZED VIEW pwtest.customernation_mv 
        AS SELECT customername, phone, nationname, marketsegment, sum(accountbalance) AS accountbalance 
        FROM pwtest.customer c 
        INNER JOIN pwtest.nation n 
        ON c.nationid = n.nationid 
        GROUP BY customername, phone, nationname, marketsegment;

-- Drop a Materialized View
    DROP MATERIALIZED VIEW pwtest.customernation_mv
    
-- Permissions (Schema/Table/Column)

-- Grant usage on a schema for a user (public schema is allowed by default)
    GRANT USAGE ON SCHEMA pwtest.customer TO pete_restricted; 
    REVOKE USAGE ON TABLE pwtest.customer FROM pete_restricted;

-- Grant & revoke select permissions on a table for a user
    GRANT SELECT ON pwtest.customer TO pete_restricted;
    REVOKE SELECT ON TABLE pwtest.customer FROM pete_restricted;

-- Grant & revoke select & update permissions for a group
    GRANT SELECT, UPDATE ON pwtest.customer TO GROUP devs;
    REVOKE SELECT, UPDATE ON TABLE pwtest.customer FROM GROUP devs;

-- Grant  & revoke select & update on 2 columns of a table for a user
    GRANT SELECT (marketsegment, accountbalance), UPDATE (marketsegment, accountbalance) ON pwtest.customer TO pete_restricted;
    REVOKE SELECT (marketsegment, accountbalance), UPDATE (marketsegment, accountbalance) ON pwtest.customer FROM pete_restricted;
    
-- Permissions (Auditing)

-- Impersonate User (requires supeuser)
    SET SESSION AUTHORIZATION 'finance';
    RESET SESSION AUTHORIZATION;

-- Show which users have column-level access control (table-scoped)
    SELECT b.attacl, b.attname, c.relname 
    FROM pg_catalog.pg_attribute_info b 
    JOIN pg_class c ON c.oid=b.attrelid 
    WHERE c.relname in ('customer','customer_vw','customernation_mv') 
    AND b.attacl IS NOT NULL 
    ORDER BY c.relname, b.attname;

-- View all Grants by Schema or User
    SELECT * 
    FROM 
        (
        SELECT 
            schemaname
            ,objectname
            ,usename
            ,HAS_TABLE_PRIVILEGE(usrs.usename, fullobj, 'select') AND has_schema_privilege(usrs.usename, schemaname, 'usage')  AS sel
            ,HAS_TABLE_PRIVILEGE(usrs.usename, fullobj, 'insert') AND has_schema_privilege(usrs.usename, schemaname, 'usage')  AS ins
            ,HAS_TABLE_PRIVILEGE(usrs.usename, fullobj, 'update') AND has_schema_privilege(usrs.usename, schemaname, 'usage')  AS upd
            ,HAS_TABLE_PRIVILEGE(usrs.usename, fullobj, 'delete') AND has_schema_privilege(usrs.usename, schemaname, 'usage')  AS del
            ,HAS_TABLE_PRIVILEGE(usrs.usename, fullobj, 'references') AND has_schema_privilege(usrs.usename, schemaname, 'usage')  AS ref
        FROM
            (
            SELECT schemaname, 't' AS obj_type, tablename AS objectname, schemaname + '.' + tablename AS fullobj FROM pg_tables
            WHERE schemaname not in ('pg_internal')
            UNION
            SELECT schemaname, 'v' AS obj_type, viewname AS objectname, schemaname + '.' + viewname AS fullobj FROM pg_views
            WHERE schemaname not in ('pg_internal')
            ) AS objs
            ,(SELECT * FROM pg_user) AS usrs
        ORDER BY fullobj
        )
    WHERE (sel = true or ins = true or upd = true or del = true or ref = true)
    and schemaname='<opt schema>'
    and usename = '<opt username>';

-- View all Grants by Group
    select relacl , 
    'grant ' || substring(
                case when charindex('r',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)) > 0 then ',select ' else '' end 
            ||case when charindex('w',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)) > 0 then ',update ' else '' end 
            ||case when charindex('a',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)) > 0 then ',insert ' else '' end 
            ||case when charindex('d',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)) > 0 then ',delete ' else '' end 
            ||case when charindex('R',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)) > 0 then ',rule ' else '' end 
            ||case when charindex('x',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)) > 0 then ',references ' else '' end 
            ||case when charindex('t',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)) > 0 then ',trigger ' else '' end 
            ||case when charindex('X',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)) > 0 then ',execute ' else '' end 
            ||case when charindex('U',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)) > 0 then ',usage ' else '' end 
            ||case when charindex('C',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)) > 0 then ',create ' else '' end 
            ||case when charindex('T',split_part(split_part(array_to_string(relacl, '|'),pu.groname,2 ) ,'/',1)) > 0 then ',temporary ' else '' end 
        , 2,10000)
    || ' on '||namespace||'.'||item ||' to "'||pu.groname||'";' as grantsql
    from 
    (SELECT 
    use.usename as subject, 
    nsp.nspname as namespace, 
    c.relname as item, 
    c.relkind as type, 
    use2.usename as owner, 
    c.relacl 
    FROM 
    pg_user use 
    cross join pg_class c 
    left join pg_namespace nsp on (c.relnamespace = nsp.oid) 
    left join pg_user use2 on (c.relowner = use2.usesysid)
    WHERE 
    c.relowner = use.usesysid  
    and  nsp.nspname   NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
    ORDER BY 
    subject,   namespace,   item 
    ) join pg_group pu on array_to_string(relacl, '|') like '%'||pu.groname||'%' 
    where relacl is not null
    and pu.groname='devs'
    order by 2
    
-- https://peter-whyte.com/redshift-cheat-sheet/
