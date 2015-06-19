set term png font "arial,9"
set datafile separator ";"
set style data linespoint
set decimalsign "."
set timefmt "%Y-%m-%d %H:%M:%S"
set xdata time
set output "30000insertsBy10Threads.png"
set yrange [0 : 30000]
set format x "%Y-%m-%d %H:%M"
set xtics rotate by -90
set ylabel 'runtime (ms)'
set title '30000 (10000 orders, 20000 partorders) inserts by 10 parallel threads'
set grid
plot '30000insertsBy10Threads.dat' using 1:2 notitle


