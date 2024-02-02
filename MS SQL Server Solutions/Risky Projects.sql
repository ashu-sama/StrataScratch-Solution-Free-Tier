with projects as
(
    select id, title, budget, Datediff(day, start_date, end_date)*1.0/365 duration
    from linkedin_projects
),
agg_emp_sal as
(
    select project_id, sum(emp.salary) sal_per_prj
    from
        linkedin_emp_projects rel 
        join linkedin_employees emp
            on rel.emp_id = emp.id
    group by project_id
)
select p.title, p.budget, ceiling(duration * sal_per_prj) prorated_employee_expense
from 
    projects p 
    join agg_emp_sal e on e.project_id = p.id
where budget < round(duration * sal_per_prj, 0)
