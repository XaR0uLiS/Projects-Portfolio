select *
from CovidDeaths
where continent is not null
order by 3,4;

--select *
--from CovidVaccinations
--order by 3,4;

-- Select the fields to start with
select 
	location, 
	date, 
	total_cases, 
	new_cases, 
	total_deaths, 
	population
from CovidDeaths
where continent is not null
-- and location = 'Greece'
order by 1,2;

-- How many Covid Cases end up with death
select 
	location, 
	date, 
	total_cases, 
	total_deaths,
	(cast(total_deaths as float) / cast(total_cases as float))*100 as DeathPercentage
from CovidDeaths
where continent is not null
-- and location = 'Greece'
order by 1,2;

-- Total Cases per Population. How many in Population were positive in Covid Test
select 
	location,
	date,
	total_cases, 
	population,
	(cast(total_cases as float) / population)*100 as PercentPopulationInfected
from CovidDeaths
where continent is not null
-- and location = 'Greece'
order by 1,2;

-- Countries with Highest Infection Rate compared to Population
select 
	location,
	population,
	max(cast(total_cases as float)) as HighestInfectionCount, 
	max((cast(total_cases as float) / population))*100 as InfectedPopulationPercentage
from CovidDeaths
where continent is not null
-- and location = 'Greece'
group by location, population
order by InfectedPopulationPercentage desc;

-- Countries with the Highest Rate and number of deaths per population
select 
	location,
	population,
	max(cast(total_deaths as float)) as HighestDeathCount, 
	max((cast(total_deaths as float) / population))*100 as DeathsPopulationPercentage
from CovidDeaths
where continent is not null
-- and location = 'Greece'
group by location, population
order by DeathsPopulationPercentage desc;

-- Highest number of Deaths per Location
select 
	location,
	max(cast(total_deaths as float)) as HighestDeathCount
from CovidDeaths
where continent is not null
-- and location = 'Greece'
group by location
order by HighestDeathCount desc;


-- Break Things Down By Continent


-- Continents with the Highest number of deaths
select 
	continent,
	max(cast(total_deaths as float)) as HighestDeathCount
from CovidDeaths
where continent is not null
-- and location = 'Greece'
group by continent
order by HighestDeathCount desc;

-- Global Numbers regarding the new cases and deaths
select  
	date, 
	sum(new_cases) as TotalCases, 
	sum(new_deaths) as TotalDeaths,
	sum(new_deaths) / sum(new_cases)*100 as DeathPercentage
from CovidDeaths
where continent is not null
group by date
having sum(new_cases) <> 0 -- condition added as we cannot divide by 0
order by date;

-- find the Overall Totals from new cases and deaths and also the percentage
select  
	sum(new_cases) as TotalCases, 
	sum(new_deaths) as TotalDeaths,
	sum(new_deaths) / sum(new_cases)*100 as DeathPercentage
from CovidDeaths
where continent is not null;


-- How many people in the world have been vaccinated
select
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	new_vaccinations
from CovidDeaths dea
join CovidVaccinations vac
on dea.location = vac.location
and dea.date = vac.date
where dea.continent is not null
order by 2,3;

-- Sum up all the new vaccination per location, much like a running total
select
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	sum(convert(bigint, new_vaccinations)) over (partition by dea.location order by dea.location, dea.date) as TotalRunningVaccinations
from CovidDeaths dea
join CovidVaccinations vac
on dea.location = vac.location
and dea.date = vac.date
where dea.continent is not null
order by 2,3;

-- Find the percentage of vaccinated people in each location
-- Using CTE method
with vaccinated_people (continent, location, date, population, new_vaccinations, TotalRunningVaccinations)
as
(
select
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	sum(convert(bigint, new_vaccinations)) over (partition by dea.location order by dea.location, dea.date) as TotalRunningVaccinations
from CovidDeaths dea
join CovidVaccinations vac
on dea.location = vac.location
and dea.date = vac.date
where dea.continent is not null
)
select
	location,
	population,
	new_vaccinations,
	TotalRunningVaccinations,
	(TotalRunningVaccinations / population) * 100 as VaccinatedPeoplePercentage
from vaccinated_people
where new_vaccinations is not null;

-- Find the Highest Percentage of Vaccinated People in each location
with vaccinated_people as
(
select
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	sum(convert(bigint, new_vaccinations)) over (partition by dea.location order by dea.location, dea.date) as TotalRunningVaccinations
from CovidDeaths dea
join CovidVaccinations vac
on dea.location = vac.location
and dea.date = vac.date
where dea.continent is not null
-- order by 2,3;
)
select
	location,
	--population,
	--new_vaccinations,
	--AddingUpVaccinsNumber,
	max((TotalRunningVaccinations / population) * 100) as VaccinatedPeoplePercentage
from vaccinated_people
where new_vaccinations is not null
group by location;

-- Using Temp Table instead of CTE
drop table if exists #PercentPopulationVaccinated;
create table #PercentPopulationVaccinated (
continent nvarchar(255),
location nvarchar(255),
date datetime,
population bigint,
new_vaccinations bigint,
TotalRunningVaccinations float
)

insert into #PercentPopulationVaccinated
select
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	sum(convert(bigint, new_vaccinations)) over (partition by dea.location order by dea.location, dea.date) as TotalRunningVaccinations
from CovidDeaths dea
join CovidVaccinations vac
on dea.location = vac.location
and dea.date = vac.date
where dea.continent is not null
-- order by 2,3;

select 
	*,
	(TotalRunningVaccinations / Population) * 100
from #PercentPopulationVaccinated
order by 2,3;


-- Create Views for later use

-- PercentPopulationVaccinated View	
create view PercentPopulationVaccinated as
select
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	sum(convert(bigint, new_vaccinations)) over (partition by dea.location order by dea.location, dea.date) as TotalRunningVaccinations
from CovidDeaths dea
join CovidVaccinations vac
on dea.location = vac.location
and dea.date = vac.date
where dea.continent is not null
-- order by 2,3;

select * from PercentPopulationVaccinated;