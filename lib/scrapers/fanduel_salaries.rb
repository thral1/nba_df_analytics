require 'open-uri'
require 'nokogiri'
require 'pry'
require 'fileutils'
require 'date'

seasons_h = {
    #"2014-15" => [ "2014-10-28", "2015-4-16", "2015-6-17" ],
    #"2015-16" => [ "2015-10-27", "2015-12-29", "2015-12-28" ]
    "2015-16" => [ "2015-12-29", "2016-01-23", "2015-12-28" ]
}

seasons_h.each{|season,dates|
  day = Date.parse dates[0]
  regseason_end = Date.parse dates[1]
  dir = FileUtils::mkdir_p season + "/fanduelsalaries"
  while day < regseason_end
    
    doc = Nokogiri::HTML( open( "http://rotoguru1.com/cgi-bin/hyday.pl?game=fd&mon=#{day.month}&day=#{day.day}&year=#{day.year}&scsv=1" ) )
    csv = doc.at_css("pre").text

    File.open( season + "/fanduelsalaries/" + "#{day.year}_#{day.month}_#{day.day}.csv", "w" ){|f|
      f.write csv
    }
    p "parsed #{day.month} #{day.day}, #{day.year}"

    day = day + 1
  end
}



