# Analisi di dati in tempo reale di una partita di calcio
## Requisiti del progetto
Lo scopo del progetto e di analizzare in tempo reale, tramite un framework open-source di data stream processing (a scelta tra Apache Flink, Apache Spark Streaming, Apache Storm, Twitter Heron) il dataset del
DEBS 2013 Grand Challenge riguardante una partita di calcio, rispondendo ad alcune query rilevanti
per gli allenatori delle due squadre e per gli spettatori della partita.
Tale dataset riguarda i data acquisiti tramite sensori wireless durante una partita di calcio tra 2 squadre
da 8 giocatori ciascuna svoltasi al Nuremberg Stadium in Germania. La partita e stata giocata su un campo 
di calcio di dimensione pari alla meta di quella standard, in due tempi della durata di 30 minuti ciascuno. 
Ciascun giocatore e l’arbitro avevano due sensori nei parastinchi, i due portieri avevano due sensori aggiuntivi
nei guanti. Il pallone aveva un sensore localizzato nel centro. I sensori nei parastinchi e nei guanti
producono dati ad una frequenza di 200Hz, quello nel pallone ad una frequenza di 2000Hz. Il tasso di dati
totale e di circa 15000 eventi di posizione al secondo. 

Il dataset ha il seguente schema:

sid , ts , x , y , z , |v| , |a| , vx , vy , vz , ax , ay , az

dove

sid ,//id del sendore

ts ,//timestamp

x , y , z ,// coordinate del sensore

|v| ,//velocità media

|a| ,// accelerazione media

vx , vy , vz ,// vettori della velocità

ax , ay , az// vettori di accelerazione

Assumendo che i dati arrivino al sistema di data stream processing senza ritardi e omissioni e supponendo
di effettuarne il replay, si chiede di rispondere alla seguenti query in tempo reale:

1. Analizzare le prestazioni nella corsa di ogni giocatore che partecipa alla partita. L’output della
statistica aggregata sulla corsa ha il seguente schema:

    ts_start, ts_stop,playerid ,totaldistance,avg_speed ;

    dove:

    ts_start, //inizio della corsa

    ts_stop, //fine della corsa

    playerid, //identificativo del giocatore 

    total_distance, //siatanza totale percorsa da un giocatore

    avg_speed //velocità media del giocatore

    Tali statistiche aggregate dovranno essere calcolate per diverse finestre temporali, in modo tale da
    permettere di confrontare le prestazioni di ciascun giocatore durante lo svolgimento della partita. Le
    finestre temporali richieste hanno durata di:

    • 1 minuto;

    • 5 minuti;

    • l’intera partita.

2. A complemento della query precedente, si richiede di fornire la classifica aggiornata in tempo reale
dei 5 giocatori piu veloci. L’output della classifica ha il seguente schema: 

    ts_start, ts_Stop, playerId1 ,avg_speed1 , playerId2 ,avg_speed2,playerId3 ,avg_speed3, playerId4 ,avg_speed4,
    playerId5 ,avg_speed5

    dove:

    ts_start, //inizio della corsa

    ts_stop, //fine della corsa

    playerid2, //identificativo del giocatore 1

    avg_speed2 //velocità media del giocatore 1

    playerid1, //identificativo del giocatore 2

    avg_speed1 //velocità media del giocatore 2
    
    . . .

    Tali statistiche aggregate dovranno essere calcolate per diverse finestre temporali, in modo tale da
    permettere di confrontare le prestazioni dei giocatori piu veloci durante lo svolgimento della partita. `
    Le finestre temporali richieste hanno durata di:

    • 1 minuto;

    • 5 minuti;

    • l’intera partita.

3. L’obiettivo della terza query e di calcolare le statistiche relative a quanto tempo ciascun giocatore tra- 
   scorre nelle diverse zone del campo di gioco, quindi una sorta di heat map. A tale scopo, si suddivide
   il campo di gioco in una griglia di celle di uguale dimensione, con 8 celle lungo l’asse x e 13 celle
   lungo l’asse y (quindi una griglia composta da 104 celle).
   Si chiede di fornire per ciascun giocatore la percentuale di tempo che il giocatore trascorre in ciascuna
   cella usando due differenti finestre temporali:

   • 1 minuto;

   • l’intera partita.

    L’output della query 3 ha il seguente schema:

    ts, playerid , cellid1 ,percent_time_in_cell_1, cellid2 ,percent_time_in_cell_2,cellid3 ,percent_time_in_cell_3,. . .

    dove:

    ts , // timestamp di inizio statistica

    playerid , // identificativo del giocatore

    cellid1 , // identificatore della cella #1

    percent_time_in_cell_1 , // percentuale di tempo che un giocatore spende nella cella #1 durante la finestra temporale

    . . .

Si chiede inoltre di valutare sperimentalmente i tempi di latenza ed il throughput delle tre query durante
il processamento sulla piattaforma di riferimento usata per la realizzazione del progetto.

**Opzionale**: Insieme ad un gruppo che ha utilizzato un altro framework di data stream processing, confrontare,
sulla stessa piattaforma di riferimento, le prestazioni in termini di tempo di latenza e throughput
delle query ottenute dai due framework.
