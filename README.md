# Конвертиране на датата във формат за данъчната декларация и поставяне на валутните курсове в съседна колона

С тези скриптове отчасти се автоматизира обработката на данните за данъчната декларация.

Въвеждането на данните в данъчната декларация може да стане чрез [nap-autopilot](https://github.com/vstoykovbg/nap-autopilot) (захранва се с csv файлове с обработени данни).

Първо вкарваме данните в електронна таблица.

С различни методи проба-грешка се установява кой метод на копиране работи:
* Дали с copy/paste или селектиране и средния бутон (в Linux).
* С кой браузър да се отварят HTML справките за да работи коректно копирането.
* С кой браузър или програма за преглед на PDF да се отварят справките във формат PDF за да работи копирането от PDF.
* Дали първо да се копира в обикновен текстов редактор и после от там да се копира и постави (copy/paste) в електронната таблица.
* Как да се постави (paste) - дали да се избере нещо от менюто Paste Special или не.

## Подводни камъни при четене на справките и грешни данни

При някои инвестиционни посредници справките в PDF формат не могат да се обработят даже и с изкуствен интелект. Например при TastyTrade - те дават машинно четими справки със закъснение (не съм ги проверявал дали са годни, но предишна година имаше грешна дата в една от машинно четимите им справки и затова внимавам).

Прочетете първо за подводните камъни при четенето на справки, има различни видове грешки в някои справки (вижте [статията ми за Interactive Brokers](https://redtapepayments.blogspot.com/2021/08/interactive-brokers-2021.html) и за [попълването на данъчната декларация](https://redtapepayments.blogspot.com/2020/10/blog-post_4.html)). Подробно обясних в цитираната статия защо смятам, че за данъчни цели (за изчисляване на печалбата от поскъпването) се взима trade date, а не settlement date.

Interactive Brokers съм ги хващал да слагат грешна дата в данъчната справка за дивидентите (обясних в коя справка е коректната дата в цитираните статии) и да не слагат някои от дивидентите в общата справка когато те са получени съвсем в края на годината (за да се видят трябва да се избере като крайна дата някой работен ден от януари следващата година - затова си вадя контролни справки с начална дата няколко дена преди началото на годината и няколко дена след края ѝ за да проверя дали има изпуснати данни).

## Данни за валутните курсове със запълнени празнини

Файловете с имена, съдържащи "corrected" са коригирани така, че да включват валутните курсове за всички дни от годината. Когато има празнина се запълва с предходния публикуван валутен курс.

Освен, че са обработени със скрипта `fill_gaps_in_currency_rates.py` допълнително съм ги коригирал да нямат нули в края (trailing zeors) за да съвпадат с оригиналните данни. И махнах заглавната част, не помня защо.

Данните във файловете с имена, съдържащи "with_gaps", са получени от сайта на БНБ (липсват данни за някои дни, защото БНБ не дава валутен курс когато е почивен ден).

## Примерно ползване
```console
$ ./convert_date_and_add_currency_rate.py USD_2023_corrected.csv input_file output_file.csv
Error: time data '' does not match format '%m/%d/%y'
```
Причината за грешката е, че последния ред от файла е празен. Иначе си работи нормално.

Данните на входа са във формата, който е ползван в справките от Interactive Brokers в HTML формат (от където копирам в електронна таблица данните за доходите): месец/ден/година.

Примерни данни за `input_file`:
```
06/12/23
06/15/23
08/24/23
08/31/23
09/07/23
09/11/23
09/15/23
11/22/23
11/30/23
12/11/23

```
Последният ред е празен, което е грешка.

На изхода получаваме във файла `output_file.csv`:

```
Original Date,Converted Date,Currency Rate
05/25/23,25.05.2023,1.82192
06/12/23,12.06.2023,1.81684
06/15/23,15.06.2023,1.80777
08/24/23,24.08.2023,1.80427
08/31/23,31.08.2023,1.79962
09/07/23,07.09.2023,1.82617
09/11/23,11.09.2023,1.82379
09/15/23,15.09.2023,1.83508
11/22/23,22.11.2023,1.79253
11/30/23,30.11.2023,1.78925
12/11/23,11.12.2023,1.81819
```
Отваряме csv файла с програмата за електронни таблици и копираме цялата колона като внимаваме да не се получи разминаване. За по-сигурно може да копираме и трите колони и така ще имаме още една колона с датата (за по-лесна визуална проврка дали погрешка сме копирали данните по-нагоре или по-надолу).

## Капани при обработката на данните

НАП работи с десетични точки, не десетични запетаи. Внимавайте във формата на числата.

Не забрайвяте във формулата за преизчисляване да сложите закръгляване до втория знак след десетичната точка - например `=ROUND(A3*C3,2)`.

![снимка на екрана на електронна таблица с формула за закръгляване]([http://url/to/img.png](https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEhH91jrvB0x5pLP-ozaNdTI6i8mTTh4ufCDsNEYzFUdaD4bDKz2y_P5Gbf6F7B9MMuIBnMKYRZXpVc_nY65YOlj-hKur2WoVxiPeLlGJKLS8e9JTaK2GLGm_mJLNtfNznRmQ00t0E0fO3KfHJyyd8CB-Pi8_ekyqCj7JkptN6MfEJS_n2FMPvs2TsCZoAWO/s409/interest-example.png)https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEhH91jrvB0x5pLP-ozaNdTI6i8mTTh4ufCDsNEYzFUdaD4bDKz2y_P5Gbf6F7B9MMuIBnMKYRZXpVc_nY65YOlj-hKur2WoVxiPeLlGJKLS8e9JTaK2GLGm_mJLNtfNznRmQ00t0E0fO3KfHJyyd8CB-Pi8_ekyqCj7JkptN6MfEJS_n2FMPvs2TsCZoAWO/s409/interest-example.png) 

Подробно как трябва да се обработят данните за данъчната декларация:

* [Отговори на въпроси за данъците и попълване на годишната данъчна декларация при инвестиции на финансовите пазари](https://redtapepayments.blogspot.com/2020/10/blog-post_4.html)

За дивидентите и лихвите сравнително лесно може да се автоматизира целия процес (ако справките не са в някой неподходящ за машинна обработка PDF формат като на TastyTrade). Но за капиталовата печалба има усложнения с данъчните лотове и определяне коя продажба трябва да се включи в изчисленията и коя не трябва (липват данни в справките за това дали сделката е минала на регулиран пазар или не, трябва да се вадят данни и от сайта на ESMA за да се провери на кой сегмент от борсата е минала сделката и дали този сегмент е вписан като regulated market или не). Има усложнения с данъчните лотове и при данните за притежавани акции и дялове към 31 декември.

В справките на Degiro има данни за MIC кода на сегмента, през който е минала сделката. Такива данни няма в спраквите на Interactive Brokers. Освен това в справките от Interactive Brokers идентификаторите на борсите не са стандартни MIC кодове и се налага да се проверява в сайта на Interactive Brokers кой идентификатор на коя борса съответства. Правете разлика между MIC код на борса и MIC код на сегмент на борса.