# Автоматична обработка на данните от Trading212

За сега имам скрипт само за дивидентите, кешбека и лихвите. Тоест за капиталовата печалба (код 508 от приложение 5) и притежаваните акции/дялове към 31 декември нямам.

ⓘ За да работят коректно трябва да изтеглите заедно с тях и някои други важни файлове и директории с файлове и те да са в текущата директория. Най-лесно е да изтеглите всичко в zip файл - от бутона Code се отваря меню, от което избирате Download ZIP (или от [този линк](https://github.com/vstoykovbg/BNB_currency_rates/archive/refs/heads/main.zip)).

(Уточнение: Електронните таблици за LibreOffice Calc работят без Питон, те си имат всичко необходимо в тях, може да свалите само таблицата, която ви трябва. Ползват макрос на Бейсик, не Питон.)

[Линк към главното README на хранилището.](README.md)

## Автоматична обработка на дивидентите от Trading212

Скриптът `process_T212_dividends_from_CSV_file.py` приема CSV файл от Trading212 за дивидентите и извежда CSV файл за nap-autopilot (или файл за разглеждане в програма за електронни таблици като LibreOffice Calc и Excel).

    $ ./process_T212_dividends_from_CSV_file.py
    Usage: ./process_T212_dividends.py input.csv output.csv [mode=nap-autopilot|sheet|table]

Има три режима на работа - nap-autopilot, sheet (за детайлен преглед на междинните резултати) и table (за любителите на ръчното преписване от електронна таблица във формуляра на данъчната декларация). При наличието на nap-autopilot няма особен смисъл от ръчното преписване. Ако не се зададе параметър mode се приема режим nap-autopilot.

За да работи скрипта трябва да има директория `currency_rates` в директорията, където е този скрипт. Препоръчително е да присъства и файла `ISIN_country.csv`, който съдържа някои съответствия между ISIN кодове и държави. Точното описване на държавите не е особено важно, от значение е декларираният данък да не е по-малко.

Когато има удържани данъци върху дивидентите резултатите са приблизителни, но това не е проблем, защото може погрешка да се декларира по-малко дължим в България данък само в случаите когато удържаният в чужбина данък е над 0%, но под 5% (не се сещам за случай когато се удържа такъв данък, много малко вероятно е да попаднете на такъв данък).

Не поправя датата, само извежда съобщение (warning), че датата може да е грешна заради разликата в часово време (вероятно GMT е за CSV справките). Другите скриптове (за лихви и кешбеци) - също.

## Автоматична обработка на лихвите от Trading212

    $ ./process_T212_interest_from_CSV_file.py 
    ERROR: Input CSV file is required.
    
    Usage:  process_T212_interest_from_CSV_file.py [mode=sheet|total] <input_csv> [output_csv]
      mode         Optional. 'sheet' to generate spreadsheet-style output, 'total' (default) for summary.
      input_csv    Required. Path to the input CSV file.
      output_csv   Optional. Path to the output CSV file.

Може да изведе csv файл с детайли:

    $ ./process_T212_interest_from_CSV_file.py input_interest.csv output-table-interest.csv mode=sheet

Или само обща сума:

    $ ./process_T212_interest_from_CSV_file.py input_interest.csv output-total-interest-value.txt mode=total

По подразбиране приема mode=total:

    $ ./process_T212_interest_from_CSV_file.py input_interest.csv output-total-interest-value.txt

Ако не се зададе име на файл за запис извежда само на екрана общата сума:

    $ ./process_T212_interest_from_CSV_file.py input_interest.csv


## Автоматична обработка на кешбеците от Trading212


    $ ./process_T212_cashback_from_CSV_file.py 
    ERROR: Input CSV file is required.
    
    Usage: process_T212_cashback_from_CSV_file.py [mode=sheet|total] <input_csv> [output_csv]
      mode=...      Optional. Choose 'sheet' or 'total' (default is 'total').
      input_csv     Required. Path to input CSV file.
      output_csv    Optional. Path to output CSV file (defaults to stdout or derived).

Може да изведе csv файл с детайли:

    $ ./process_T212_cashback_from_CSV_file.py input_cashback.csv output-table-cashback.csv mode=sheet

Или само обща сума:

    $ ./process_T212_cashback_from_CSV_file.py input_cashback.csv output-total-cashback-value.txt mode=total

По подразбиране приема mode=total:

    $ ./process_T212_cashback_from_CSV_file.py input_cashback.csv output-total-cashback-value.txt

Ако не се зададе име на файл за запис извежда само на екрана общата сума:

    $ ./process_T212_cashback_from_CSV_file.py input_cashback.csv

## Има ли автоматичен начин да обработя данните за капиталовата печалба и притежаваните към 31 декември акции и дялове?

Може да ползвате безплатната версия на NRA Assist за доходите код 508 (приложение 5) и притежаваните акции/дялове към 31 декември. Но имайте предвид, че трябва ръчно да прегледате всяка една продажба (за която има шанс да е възможно данъчно изключение) дали е на регулиран пазар и ако е на регулиран пазар - да проверите дали има данъчно изключение.

Авторът на NRA Assist твърди, че системата му правилно изчислява капиталовата печалба за Trading212 като отчита stock splits (но все пак ръчно трябва да проверите всяка продажба дали е с данъчно изключение, данни за това няма в .csv справката, трябва да търсите потвържденията за продажбите и да гледате дали са TOTV или OTC на тези продажби, за които има шанс да има данъчно изключение ако минат борсово, след това да проверявате [дали съответния инструмент на съответната борса е минал на регулиран пазар](https://redtapepayments.blogspot.com/2024/02/blog-post.html)). Разбира се за да работи изчислението трябва да захраните системата с всички покупки и продажби, не само за една година (ако има продажби на инструменти купени през предишни години).

Да сгрешите при деклариране на притежаваните към 31 декември акции и дялове не е голям проблем, защото това деклариране не влияе на данъчните ни задължения и не е предвидена глоба ако не сме укрили данък или не сме декларирали нещо (напр. дадени/получени заеми), за което изрично има предвидена глоба в закона.

Важно е да няма грешка в деклариране на доходитепо такъв начин, че да се декларира по-нисък от действително дължимия данък.

[Линк към главното README на хранилището.](README.md)
