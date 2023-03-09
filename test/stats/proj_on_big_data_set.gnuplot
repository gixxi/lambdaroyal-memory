set term png font "arial,9"
set datafile separator ";"
set style data linespoint
set decimalsign "."
set timefmt "%Y-%m-%d %H:%M:%S"
set xdata time
set output "proj_on_big_data_set.png"
set format x "%Y-%m-%d %H:%M"
set xtics rotate by -90
set multiplot layout 2,1 rowsfirst
set yrange [0 : 70000]
set ylabel 'runtime (ms)'
set title 'Projection from 5000 articels to their stocks after inserting 50000 articles with 5000 stocks'
set grid
plot 'proj_on_big_data_set.dat' using 1:2 title 'insert (ms)'

# second plot

unset title

# set margins to match first plot
set bmargin 2.5
set tmargin 0

set yrange [0 : 100]
plot 'proj_on_big_data_set.dat' using 1:3 title 'projection (ms)'
