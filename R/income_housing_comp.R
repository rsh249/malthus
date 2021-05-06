library(ggplot2)
library(dplyr)
library(readxl)
library(stringr)
library(tidyr)

## income ----
# median income data by state (US Census: https://www.census.gov/data/tables/time-series/demo/income-poverty/historical-income-households.html)
download.file("https://www2.census.gov/programs-surveys/cps/tables/time-series/historical-income-households/h08.xlsx", 'h08.xlsx')
income = read_xlsx('h08.xlsx')
evens = seq(2, ncol(income), 2)
trimincome = income[4:nrow(income), c(1, evens)]
colnames(trimincome) = str_sub(trimincome[1,], 1, 4)
trimincome = trimincome[c(-1,-2),]
# use only the first rows for "Current Dollars" (CPI adjusted, Not inflation adjusted I think)
trimincome = trimincome[1:52,]
income_table = pivot_longer(trimincome, cols=colnames(trimincome)[2:ncol(trimincome)])
colnames(income_table) <- c('state', 'year', 'income')
income_table$year = as.numeric(income_table$year)
income_table$income = as.numeric(income_table$income)

# create an income index
income_table = income_table %>% 
  filter(year>=1989) %>% 
  group_by(state) %>% 
  mutate(II=(income/last(income)))


## median home price index from FHFA https://www.fhfa.gov/DataTools/Downloads/Pages/House-Price-Index-Datasets.aspx ----

medhome = read.csv('https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_state.csv', header=F)
colnames(medhome) = c('state', 'year', 'Q', 'HPI')
repl_state <- function(x)  state.name[which(state.abb == x)]
medhome$state = as.vector(lapply(medhome$state, repl_state))
medhome$state = as.character(medhome$state)

medhome = medhome %>% 
  group_by(state) %>% 
  filter(year>=1989) %>% 
  filter(Q==1) %>%
  mutate(HPI89=HPI/first(HPI))

## Join ----
joints = inner_join(income_table, medhome, by=c('state', 'year'))


## plotting ----

findst = c('Connecticut', 
           'Massachusetts')

ggplot(filter(joints, state %in% findst)) +
  geom_path(aes(x=year, y=II, col=state)) + 
  theme_minimal()

ggplot(filter(joints, state %in% findst)) +
  geom_path(aes(x=year, y=HPI89, col=state)) +
  geom_path(aes(x=year, y=II, col=state))

# by state plot the regression of income vs price

ggplot(filter(joints, state %in% findst))+
  geom_path(aes(x=year, y=(II/HPI89)*100, col=state)) #income index as percent of HPI: OR how much has the gap changed?


## map ----
mapjoints = joints
mapjoints$state = tolower(mapjoints$state)
colnames(mapjoints) = c('region', 'year', 'income', 'II', 'Q', 'HPI', 'HPI89')
states_map <- map_data("state")
ratio_map <- left_join(states_map, mapjoints, by = "region")

# Create the map
ggplot(ratio_map %>% filter(year==2019), aes(long, lat, group = group))+
  geom_polygon(aes(fill = II/HPI89), color = "white")+
  scale_fill_viridis_c(option = "C") 


