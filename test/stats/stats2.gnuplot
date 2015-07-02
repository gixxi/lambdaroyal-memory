set term png font "arial,9"
set datafile separator ";"
set style data linespoint
set decimalsign "."
set timefmt "%Y-%m-%d %H:%M:%S"
set xdata time
set output "1000Orders.png"
set yrange [0 : 25000]
set format x "%Y-%m-%d %H:%M"
set xtics rotate by -90
set ylabel 'runtime (ms)'
set title 'Roughly 75000 inserts, building document trees and data hierachie'
set grid
plot '1000Orders.dat' using 1:2 title 'insert', \
  '1000Orders.dat' using 1:3 title 'building document trees', \
  '1000Orders.dat' using 1:4 title 'building counting search hierarchie', \
  '1000Orders.dat' using 1:5 title 'building data search hierarchie'





