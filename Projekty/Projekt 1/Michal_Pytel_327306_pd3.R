### Przetwarzanie Danych Ustrukturyzowanych 2023L
### Praca domowa nr. 3
###
### UWAGA:
### nazwy funkcji oraz ich parametrow powinny pozostac niezmienione.
###  
### Wskazane fragmenty kodu przed wyslaniem rozwiazania powinny zostac 
### zakomentowane
###

# -----------------------------------------------------------------------------#
# Wczytanie danych oraz pakietow.
# !!! Przed wyslaniem zakomentuj ten fragment
# Posts <- read.csv("Dane/Posts.csv.gz")
# Comments <- read.csv("Dane/Comments.csv.gz")
# Users <- read.csv("Dane/Users.csv.gz")
# -----------------------------------------------------------------------------#
#install.packages("sqldf")
#install.packages("dplyr")
#install.packages("data.table")
#install.packages("microbenchmark")

# library(sqldf)
# library(dplyr)
# library(microbenchmark)
# library(data.table)

# -----------------------------------------------------------------------------#
# Zadanie 1
# -----------------------------------------------------------------------------#

sql_1 <- function(Users){
  #Użycie sqldf do wykonania zapytania sql
  sqldf("
    SELECT Location, SUM(UpVotes) as TotalUpVotes
    FROM Users
    WHERE Location != ''
    GROUP BY Location
    ORDER BY TotalUpVotes DESC
    LIMIT 10
  ") 
}


base_1 <- function(Users){
  df <- Users[Users$Location != '', ] # wykluczamy wszystkie wiersze gdzie Location jest puste
  df <- aggregate(UpVotes ~ Location, data = df, FUN = sum) # grupowanie ramki danych po Location i obliczanie sumy Upvotes dla każdej Lokalizacji
  df <- df[order(-df$UpVotes), ]# sortowanie ramki danych po zsumowanych Upvotes w sposób malejący
  colnames(df)[2] <- "TotalUpVotes" # zmiana nazwy kolumny UpVotes na TotalUpVotes
  result <- head(df, n = 10) # koncowy wynik jako górne 10 wierszy
  rownames(result) <- NULL # resetowanie licznika wierszy
  return(result) 
}


dplyr_1 <- function(Users){
  
  
  result <- Users %>%
    filter(Location != "") %>% # wykluczamy wszystkie wiersze gdzie Location jest puste
    group_by(Location) %>% # grupujemy wiersze po kolumnach Location
    summarize(TotalUpVotes = sum(UpVotes)) %>% # sumujemy głosy UpVotes po Location
    arrange(desc(TotalUpVotes)) %>% # sortowanie malejąco zsumowanych głosów
    slice_head(n = 10) # przycięcie wyniku do 10 pierwszych wyników

  result <- as.data.frame(result) # zamieniamy wynik końcowy jako ramkę danych

  return(result)
  
  
  
}

table_1 <- function(Users){
   
  dt <- as.data.table(Users) # konwertowanie data frame na data.table
  # z ramki danych wykluczamy te wiersze gdzie Location jest puste, sumujemy Upvotes przypisując nową nazwę
  # i grupujemy po Location, sortujemy malejąco po TotalUpVotes i obcinamy do pierwszych 10 wyników
  result <- dt[Location != "", .(TotalUpVotes = sum(UpVotes)), by = Location][order(-TotalUpVotes)][1:10]
  result <- as.data.frame(result) # konwertowanie z powrotem na data frame
  return(result)
}

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
# sql_1(Users)
# base_1(Users)
# dplyr_1(Users)
# table_1(Users)
# 
# all_equal(sql_1(Users), base_1(Users)) #TRUE
# all_equal(sql_1(Users), dplyr_1(Users)) #TRUE
# all_equal(sql_1(Users), table_1(Users)) #TRUE
# 
# compare(sql_1(Users), base_1(Users)) #TRUE
# compare(sql_1(Users), dplyr_1(Users)) #TRUE
# compare(sql_1(Users), table_1(Users)) #TRUE


# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
# microbenchmark::microbenchmark(
#   sqldf = sql_1(Users),
#   base = base_1(Users),
#   dplyr = dplyr_1(Users),
#   data.table = table_1(Users),
#   times = 25)

 #Unit: miliseconds
# Unit: milliseconds
#       expr      min       lq      mean   median       uq      max neval
#      sqldf 338.3878 343.5428 352.88600 347.8455 352.2852 417.0120    25
#       base 235.8222 239.4764 243.01644 241.4051 244.6909 260.0071    25
#      dplyr  55.8584  57.4041  67.60250  57.9198  61.2797 136.5580    25
# data.table  21.3599  21.7819  22.52863  21.9030  22.3445  29.4248    25

# -----------------------------------------------------------------------------#
# Zadanie 2
# -----------------------------------------------------------------------------#

sql_2 <- function(Posts){
  #Użycie sqldf do wykonania zapytania sql   
  sqldf("SELECT STRFTIME('%Y', CreationDate) AS Year, STRFTIME('%m', CreationDate) AS Month,
        COUNT(*) AS PostsNumber, MAX(Score) AS MaxScore
        FROM Posts
        WHERE PostTypeId IN (1, 2)
        GROUP BY Year, Month
        HAVING PostsNumber > 1000")
  
}

base_2 <- function(Posts){
  
  df <- Posts
  df$CreationDate <- as.Date(df$CreationDate) # Konwertowanie CreationDate na formatowanie daty
  df$Year <- format(df$CreationDate, "%Y") # stworzenie nowej kolumny o nazwie Year, która posiada pole z rokiem z CreationDate
  df$Month <- format(df$CreationDate, "%m") # stworzenie nowej kolumny o nazwie Month, króra posiada pole z miesiącem z CreationDate
  df <- subset(df, PostTypeId %in% c(1, 2)) # Wybieramy wiersze gdzie PostTypeId to 1 lub 2
  df <- split(df, list(df$Year, df$Month)) # Grupowanie po Year i Month
  
  # Obliczanie liczby postów i maksymalnego Score dla każdej grupy
  df <- lapply(df, function(group) {
    if (nrow(group) == 0) {
      return(NULL) # Jeżli liczba wierszy to 0 to zwróć null dla tej grupy, a to powoduje usunięcie tej grupy
    }
    
    return(data.frame( # jeżeli wiemy, że dana grupa nie jest pusta, to tworzymy ramkę danych
      Year = unique(group$Year), # bierze unikalne wartości dla Year
      Month = unique(group$Month), # bierze unikalne wartosci dla Month
      PostsNumber = nrow(group), # liczy ilość postów na podstawie ilości wierszy
      MaxScore = max(group$Score) # znalduję wartość maksymalną ze Score dla grupy 
    ))
  })
  df <- do.call(rbind, df) # Łączenie z powrotem w ramkę danych po wierz?
  df <- subset(df, PostsNumber > 1000) # Wybieranie podzbioru gdzie PostNumber > 1000
  result <- df[order(df$Year, df$Month), ] # Wierszowe sortowanie po Year i potem Month w sposób rosnący
  rownames(result) <- NULL # reset licznika wierszy
  return(result)
  
  
}

dplyr_2 <- function(Posts){
  result <- Posts %>%
    filter(PostTypeId %in% c(1, 2)) %>% # wybieranie postów tylko z PostTypeId 1 i 2
    mutate(Year = format(as.Date(CreationDate), "%Y"),
           Month = format(as.Date(CreationDate), "%m")) %>% # wyciąganie roku i miesiąca z CreationDate i tworznie nowych kolumn Year i Month
    group_by(Year, Month) %>% # grupowanie po roku a potem po miesiącu
    summarize(PostsNumber = n(),
              MaxScore = max(Score), .groups = "drop") %>% # liczenie ilości postów i znajdowanie maksymalnego Score
    filter(PostsNumber > 1000) # interesują nas wiersze gdzie PostNumber > 1000
  
  result <- as.data.frame(result) # konwertowanie z powrotem na ramkę danych, ponieważ w trakcie został prekształcony w tibble
  return(result)
}

table_2 <- function(Posts){
  dt <- data.table(Posts) # Konwertowanie z data.frame na data.table
  # tworzymy dwie nowe kolumny Year i Month, dajemy do nich warotści z CreationDate 
  dt[, `:=`(Year = as.character(year(CreationDate)), Month = sprintf("%02d", month(CreationDate)))]
  # wybieranie postów z PostTypeId 1 lub 2 i licznie ilości postów i maksymalny score dla każdej grupy, 
  # która jest grupowana po roku potem po miesiącu
  result <- dt[PostTypeId %in% c(1, 2),
                  .(PostsNumber = .N, MaxScore = max(Score)),
                  by = .(Year, Month)]
  result <- result[PostsNumber > 1000] # interesują nas wyniki, gdzie PostNumber jest większe niż 1000
  setorder(result, Year, Month) # Sortowanie po year i potem po month
  result <- as.data.frame(result) # Konwersja na data.frame 
  return(result)
  
}

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
# sql_2(Posts)
# base_2(Posts)
# dplyr_2(Posts)
# table_2(Posts)

# all_equal(sql_2(Posts), dplyr_2(Posts)) #TRUE
# all_equal(sql_2(Posts), base_2(Posts)) #TRUE
# all_equal(sql_2(Posts), table_2(Posts)) #TRUE
# 
# compare(sql_2(Posts), dplyr_2(Posts)) #TRUE
# compare(sql_2(Posts), base_2(Posts)) #TRUE
# compare(sql_2(Posts), table_2(Posts)) #TRUE

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
# microbenchmark::microbenchmark(
#   sqldf = sql_2(Posts),
#   base = base_2(Posts),
#   dplyr = dplyr_2(Posts),
#   data.table = table_2(Posts),
#   times = 25
# )
# Unit: seconds
#       expr      min       lq     mean   median       uq      max neval
#      sqldf 2.122523 2.146237 2.197519 2.167691 2.256245 2.346184    25
#       base 2.177204 2.241994 2.302781 2.270383 2.324397 2.569841    25
#      dplyr 2.876809 2.931282 2.994451 2.966891 3.055043 3.243097    25
# data.table 7.206401 7.267174 7.382412 7.316278 7.388167 8.304170    25

# -----------------------------------------------------------------------------#
# Zadanie 3
# -----------------------------------------------------------------------------#


sql_3 <- function(Posts, Users){
  #Użycie sqldf do wykonania zapytania sql (wklejone z outlooka, nie z mooldla)   
  Questions <- sqldf( 'SELECT OwnerUserId, SUM(ViewCount) as TotalViews
                                    FROM Posts
                                    WHERE PostTypeId = 1
                                    GROUP BY OwnerUserId' )
  sqldf( "SELECT Id, DisplayName, TotalViews
              FROM Questions
              JOIN Users
              ON Users.Id = Questions.OwnerUserId
              ORDER BY TotalViews DESC
              LIMIT 10")
}
  

base_3 <- function(Posts, Users){
    
  Questions <- Posts[Posts$PostTypeId == 1, ] # Bierzemy wiersze gdzie PostTypeId to 1
  # Sumujemy ilość wyświetleń postów dla każdego użytkownika
  Questions <- aggregate(Questions$ViewCount, by = list(OwnerUserId = Questions$OwnerUserId), FUN = sum)
  colnames(Questions)[2] = "TotalViews" #zmieniamy nazwę drugiej kolumny na TotalViews
  result <- merge(Users, Questions, by.x = "Id", by.y = "OwnerUserId") # Łączymy Users i Questions po Id użytkownika
  result <- result[order(-result$TotalViews),] # Sortujemy ramkę danych po TotalViews malejąco
  result <- result[1:10, c("Id", "DisplayName", "TotalViews")] # Wybieramy górne 10 wyników z 3 podanymi kolumnami
  rownames(result) <- NULL # reset licznika wierszy
  
  return(result)
}

dplyr_3 <- function(Posts, Users){
  result <- Posts %>%
    filter(PostTypeId == 1) %>% # Bierzemy wiersze gdzie PostTypeId to 1
    group_by(OwnerUserId) %>% # Grupowanie po Id użytkownika
    summarise(TotalViews = sum(ViewCount, na.rm = TRUE)) %>% # Sumujemy ilość wyświetleń postów dla każdego użytkownika
    inner_join(Users, by = c("OwnerUserId" = "Id")) %>% # Łączymy Users i Questions po Id użytkownika
    arrange(desc(TotalViews)) %>% # Sortujemy ramkę danych po TotalViews malejąco
    select(Id = OwnerUserId, DisplayName, TotalViews) %>% # Wybieramy 3 podane kolumny
    slice_head(n = 10) #Wybieramy górne 10 wierszy 
  
  return(as.data.frame(result))
 
}

table_3 <- function(Posts, Users){

  # Konwersja data.frame (Posts, Users) na data.table
  Posts <- as.data.table(Posts)
  Users <- as.data.table(Users)
  
  result <- Posts[PostTypeId == 1, # Bierzemy wiersze gdzie PostTypeId to 1
    .(TotalViews = sum(ViewCount)), # Sumujemy ilość wyświetleń postów dla każdego użytkownika
    by = OwnerUserId # Grupowanie po Id użytkownika
  ][Users, on = .(OwnerUserId = Id) # Łączymy Users i Questions po Id użytkownika
  ][order(-TotalViews) # Sortujemy ramkę danych po TotalViews malejąco
  ][1:10, #Wybieramy górne 10 wierszy 
    .(Id = OwnerUserId,DisplayName,TotalViews)] # Wybieramy 3 podane kolumny
  
  result <- as.data.frame(result) #konwersja wynikowej data.table na data.frame
  return(result)
}

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
# sql_3(Posts, Users)
# base_3(Posts, Users)
# dplyr_3(Posts, Users)
# table_3(Posts, Users)

# all_equal(sql_3(Posts, Users), base_3(Posts, Users)) #TRUE
# all_equal(sql_3(Posts, Users), dplyr_3(Posts, Users)) #TRUE
# all_equal(sql_3(Posts, Users), table_3(Posts, Users)) #TRUE
# 
# compare(sql_3(Posts, Users), base_3(Posts, Users)) #TRUE
# compare(sql_3(Posts, Users), dplyr_3(Posts, Users)) #TRUE
# compare(sql_3(Posts, Users), table_3(Posts, Users)) #TRUE

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem

# microbenchmark::microbenchmark(
#   sqldf = sql_3(Posts, Users),
#   base = base_3(Posts, Users),
#   dplyr = dplyr_3(Posts, Users),
#   data.table = table_3(Posts, Users),
#   times = 25
# )

# Unit: milliseconds
#       expr       min        lq       mean    median        uq       max neval
#      sqldf 2420.7251 2450.7951 2463.22801 2455.8903 2465.4966 2576.9073    25
#       base  605.1986  610.7103  648.87240  621.8472  678.6002  769.6800    25
#      dplyr  131.8616  137.0589  157.14066  139.7305  193.4574  222.1828    25
# data.table   68.9837   71.2001   97.19026   74.5568  138.3596  203.0669    25

# -----------------------------------------------------------------------------#
# Zadanie  4
# -----------------------------------------------------------------------------#


sql_4 <- function(Posts, Users){
    # Tu umiesc rozwiazanie oraz komentarze
    # 
    sqldf("SELECT DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes
          FROM (
          SELECT *
          FROM (
          SELECT COUNT(*) as AnswersNumber, OwnerUserId
          FROM Posts
          WHERE PostTypeId = 2
          GROUP BY OwnerUserId
          ) AS Answers
          JOIN
          (
          SELECT COUNT(*) as QuestionsNumber, OwnerUserId
          FROM Posts
          WHERE PostTypeId = 1
          GROUP BY OwnerUserId
          ) AS Questions
          ON Answers.OwnerUserId = Questions.OwnerUserId
          WHERE AnswersNumber > QuestionsNumber
          ORDER BY AnswersNumber DESC
          LIMIT 5
          ) AS PostsCounts
          JOIN Users
          ON PostsCounts.OwnerUserId = Users.Id
          ")
}

base_4 <- function(Posts, Users){

  Answers <- Posts[Posts$PostTypeId == 2, ] # Wybieramy odpowiedzi z Posts
  Questions <- Posts[Posts$PostTypeId == 1, ] # Wybieramy pytania z Posts

  Answers <- aggregate(list(AnswersNumber = Answers$OwnerUserId), by = list(OwnerUserId = Answers$OwnerUserId), FUN = length) # Liczymy ilość odpowiedzi użytkownika
  Questions <- aggregate(list(QuestionsNumber = Questions$OwnerUserId), by = list(OwnerUserId = Questions$OwnerUserId), FUN = length) # Liczymy ilość pytań użytkownika

  result <- merge(Answers, Questions, by = "OwnerUserId") # Łączymy obie ramki danych przez Id użytkownika
  result <- result[result$AnswersNumber > result$QuestionsNumber, ] # Wybieramy te wiersze gdzie AnswerNumber jest większa niż QuestionsNumber
  result <- merge(Users, result, by.x = "Id", by.y = "OwnerUserId") # Łączymy z Users po id użytkownika 
  result <- result[, c("DisplayName", "QuestionsNumber", "AnswersNumber", "Location", "Reputation", "UpVotes", "DownVotes")] # wybieramy kolumny które nas interesują
  result <- result[order(result$AnswersNumber, decreasing = TRUE), ] # Sortujemy po AnswerNumber malejąco 
  result <- head(result, 5) # Wybieramy górne 5 wyników 
  rownames(result) <- NULL # Resetujemy licznik wierszy
  
  return(result)
}

dplyr_4 <- function(Posts, Users){

  Answers <- Posts %>%
    group_by(OwnerUserId) %>%
    summarize(AnswersNumber = sum(PostTypeId == 2)) # Liczymy sumę odpowiedzi dla każdego użytkownika
  
  Questions <- Posts %>%
    group_by(OwnerUserId) %>%
    summarize(QuestionsNumber = sum(PostTypeId == 1)) # Liczymy sumę zapytań dla każdego użytkownika 
  
  Posts_Count <- inner_join(Answers, Questions, by = "OwnerUserId") %>% # Łączymy Answers i Questions po OwnersUserId
    filter(AnswersNumber > QuestionsNumber) %>% # wybieramy wiersze gdzie AnswersNumber > QuestionsNumber
    arrange(desc(AnswersNumber)) # Sortujemy malejąco po AnsersNumber

  result <- inner_join(Posts_Count, Users, by = c("OwnerUserId" = "Id")) %>% # Łączymy Posts_Count z Users po id użytkownika
    select(DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes) %>% # wybieramy kolumny które nas interesują
    slice_head(n = 5) # zawężamy wynik do pierwszych 5 miejsc
  
  result <- as.data.frame(result) # zmieniamy na ramkę danych ponieważ zostało zamienione na tibble podczas wcześniejszych operacji
  
  return(result)
}

table_4 <- function(Posts, Users){
  
  # Bierzemy ramki danych i konwertujemy je na tablice danych
  Posts <- as.data.table(Posts) 
  Users <- as.data.table(Users)
  
  Answers <- Posts[PostTypeId == 2, .(AnswersNumber = .N), by = OwnerUserId] # Tworzymy data.table dla odpowiedzi i zliczamy je po używkownikach
  Questions <- Posts[PostTypeId == 1, .(QuestionsNumber = .N), by = OwnerUserId] # Tworzymy data.table dla pytań i zliczamu je po użytkownikach
  
  # To z merge jest szybsze dlatego postanowiłem je zostawić, chociaż to drugie ma składnie bardziej typową dla data.table
  # Zostawiam je ponieważ nie jestem pewien czy tak można mimo, że wiem że merge też jest w bibliotece data.table
  
  Posts_Count <- merge(Answers,Questions, by = "OwnerUserId")[AnswersNumber > QuestionsNumber] # Łączymy dwie tablice danych Answers i Questions po Id użytkowników i wybieramy tylko te wiersze gdzie AnswersNumber > QuestionsNumber
  # Posts_Count <- Answers[Questions, on = .(OwnerUserId)][AnswersNumber > QuestionsNumber]
  
  # Tego samego zabiegu dokonałem w tym miejscu, ponieważ zauważyłem że w microbenchmarku jest aż 2,5 razy szybsze tym sposobem
  # result <- Posts_Count[Users, on = .(OwnerUserId = Id)][
    # , .(DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes)]
  result <- merge(Posts_Count, Users, by.x = "OwnerUserId", by.y = "Id")[ # Łączymy Posts_Count i Users po Id Uzytkowników
    , .(DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes)] # wybieramy kolumny które nas interesują
  result <- result[order(-AnswersNumber)][1:5] # Sortujemy malejąco po AnswerNumber a potem wybieramy pierwsze 5 pozycji
  
  return(as.data.frame(result)) # zwracamy jako data.frame
  
}

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
# sql_4(Posts, Users)
# base_4(Posts, Users)
# dplyr_4(Posts, Users)
# table_4(Posts, Users)
# 
# all_equal(sql_4(Posts, Users), base_4(Posts, Users)) #TRUE
# compare(sql_4(Posts, Users), base_4(Posts, Users)) #TRUE
# 
# all_equal(sql_4(Posts, Users), dplyr_4(Posts, Users)) #TRUE
# compare(sql_4(Posts, Users), dplyr_4(Posts, Users)) #TRUE
# 
# all_equal(sql_4(Posts, Users), table_4(Posts, Users)) #TRUE
# compare(sql_4(Posts, Users), table_4(Posts, Users)) #TRUE

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem

# microbenchmark::microbenchmark(
#   sqldf = sql_4(Posts, Users),
#   base = base_4(Posts, Users),
#   dplyr = dplyr_4(Posts, Users),
#   data.table = table_4(Posts, Users),
#   times = 25
# )
# Unit: milliseconds
#       expr       min        lq       mean    median        uq       max neval
#      sqldf 2459.5241 2468.0842 2484.92621 2475.3382 2493.2388 2561.2385    25
#       base  800.1710  810.8269  855.10043  855.2824  890.8901  923.5562    25
#      dplyr  356.5947  387.9162  439.61532  445.0925  461.8133  673.9451    25
# data.table   41.7689   43.2242   52.21856   44.3473   46.0942  138.1046    25

# -----------------------------------------------------------------------------#
# Zadanie 5
# -----------------------------------------------------------------------------#

sql_5 <- function(Posts, Comments, Users){
  #Użycie sqldf do wykonania zapytania sql, ale jest to wklejone z outlooka, nie z moodle  
  
    
    CmtTotScr <- sqldf( 'SELECT PostId, SUM(Score) AS CommentsTotalScore
                                      FROM Comments
                                      GROUP BY PostId' )
    
    PostsBestComments <- sqldf( 'SELECT Posts.OwnerUserId, Posts.Title, Posts.CommentCount, Posts.ViewCount,

    CmtTotScr.CommentsTotalScore
                                                       FROM CmtTotScr
                                                       JOIN Posts ON Posts.Id = CmtTotScr.PostId
                                                       WHERE Posts.PostTypeId=1' )
    sqldf( 'SELECT Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location
                 FROM PostsBestComments
                 JOIN Users ON PostsBestComments.OwnerUserId = Users.Id
                 ORDER BY CommentsTotalScore DESC
                 LIMIT 10' )
  
  
}

base_5 <- function(Posts, Comments, Users){
    
  CmtTotScr <- aggregate(Score ~ PostId, data = Comments, sum) # Liczenie całościowej sumy punktów dla każdego komentarza
  colnames(CmtTotScr) <- c("PostId", "CommentsTotalScore") # Zmiana nazwy kolumn
  
  PostsBestComments <- merge(Posts, CmtTotScr, by.x = "Id", by.y = "PostId")  # Połączenie comments total score z ramką danych Posts po Id postu
  # wybieramy posty tylko co mają PostTypeId == 1 i wybieramy podane kolumny
  PostsBestComments <- PostsBestComments[PostsBestComments$PostTypeId == 1, c("OwnerUserId", "Title", "CommentCount", "ViewCount", "CommentsTotalScore")]
  
  result <- merge(PostsBestComments, Users, by.x = "OwnerUserId", by.y = "Id") # Łączymy PostsBestComments z ramką danych Users po Id użytkownika
  result <- result[, c("Title", "CommentCount", "ViewCount", "CommentsTotalScore", "DisplayName", "Reputation", "Location")] # Wybieramy podane kolumny
  result <- result[order(-result$CommentsTotalScore), ] # Sortujemy malejąco według CommentsTotalScore
  result <- head(result, 10) # Wybieramy 10 pierwszych wyników
  row.names(result) <- NULL # Resetujemy licznik wierszy
  
  return(result)
  
}

dplyr_5 <- function(Posts, Comments, Users){

  CmtTotScr <- Comments %>%
    group_by(PostId) %>% # Grupowanie po Postach
    summarise(CommentsTotalScore = sum(Score)) # Liczenie całościowej sumy punktów dla każdego komentarza

  PostsBestComments <- Posts %>%
    inner_join(CmtTotScr, by = c("Id" = "PostId")) %>% # Połączenie comments total score z ramką danych Posts po Id postu
    filter(PostTypeId == 1) %>% # wybieramy posty tylko co mają PostTypeId == 1
    select(OwnerUserId, Title, CommentCount, ViewCount, CommentsTotalScore) # wybieramy podane kolumny

  result <- PostsBestComments %>%
    inner_join(Users, by = c("OwnerUserId" = "Id")) %>% # Łączymy PostsBestComments z ramką danych Users po Id użytkownika
    select(Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location) %>% # Wybieramy podane kolumny
    arrange(desc(CommentsTotalScore)) %>% # Sortujemy malejąco według CommentsTotalScore
    slice_head(n = 10) # Wybieramy 10 pierwszych wyników
  
  
  return(result)
}

table_5 <- function(Posts, Comments, Users){
  
  # Konwersja data.frame (Posts,Comments, Users) na data.table
  Posts <- as.data.table(Posts)
  Comments <- as.data.table(Comments)
  Users <- as.data.table(Users)
  
  CmtTotScr <- Comments[, .(CommentsTotalScore = sum(Score)), by = PostId] # Liczenie całościowej sumy punktów dla każdego komentarza

  PostsBestComments <- Posts[CmtTotScr, on = .(Id = PostId)][ # Połączenie comments total score z ramką danych Posts po Id postu
    PostTypeId == 1, .(OwnerUserId, Title, CommentCount, ViewCount, CommentsTotalScore)] # wybieramy posty tylko co mają PostTypeId == 1 i wybieramy podane po kropce kolumny

  # Dołączenie tablicy danych Users, posortowanie po CommentsTotalScore w sposób malejący, wybranie 10 pierwszych wyników i wybranie podanych kolumn
  result <- PostsBestComments[Users, on = .(OwnerUserId = Id)][order(-CommentsTotalScore)][
    1:10, .(Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location)]
  
  result <- as.data.frame(result) # Przekonwertowanie data.table na data.frame
  
  return(result)
}


# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem 

# sql_5(Posts, Comments, Users) 
# base_5(Posts, Comments, Users)
# dplyr_5(Posts, Comments, Users)
# table_5(Posts, Comments, Users)
# 
# all_equal(sql_5(Posts, Comments, Users), base_5(Posts, Comments, Users)) # True
# compare(sql_5(Posts, Comments, Users), base_5(Posts, Comments, Users))# True
# 
# all_equal(sql_5(Posts, Comments, Users), dplyr_5(Posts, Comments, Users))# True
# compare(sql_5(Posts, Comments, Users), dplyr_5(Posts, Comments, Users))# True
# 
# all_equal(sql_5(Posts, Comments, Users), table_5(Posts, Comments, Users))# True
# compare(sql_5(Posts, Comments, Users), table_5(Posts, Comments, Users))# True

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem

# microbenchmark::microbenchmark(
#   sqldf = sql_5(Posts, Comments, Users),
#   base = base_5(Posts, Comments, Users),
#   dplyr = dplyr_5(Posts, Comments, Users),
#   data.table = table_5(Posts, Comments, Users),
#   times = 100
# )

#   Unit: milliseconds
#        expr       min        lq      mean     median        uq       max neval
#       sqldf 3039.7296 3061.1406 3124.7150 3085.8211 3143.5645 4307.3201   100
#        base 1835.4936 1938.4475 1980.3016 1958.4757 2002.3921 2283.6087   100
#       dplyr  380.2667  410.3677  456.8793  434.9839  485.4755  766.5854   100
#  data.table  126.4751  129.7040  159.3901  134.6800  208.0825  293.2798   100




