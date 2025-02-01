# Питонски скриптове за теглене на валутни курсове от сайта на БНБ и слагането им в csv таблици, електронни таблици .ods за улесняване попълването на годишната данъчна декларация на физическите лица в България

След като направих електронните таблици питонските скриптове е малко вероятно да ви потрябват освен скрипта за теглене на валутните курсове от сайта на БНБ.

## BNB_downloader.py - скрипт за теглене директно от сайта на БНБ

Ако се ползва този скрипт няма нужда от `fill_gaps_in_currency_rates.py`, защото освен че тегли данните директно от сайта на БНБ запълва и празнините.

Скриптът тегли датите от декември месец предната година и всички месеци от зададената година като ползва данните от миналата година за да запълни празнините в началото на януари.

Скриптът изчаква случаен интервал между 1 и 3 секудни преди всяко теглене за да не натоварва сайта на БНБ (да не се задейства някоя защита против претоварване).

Пример за теглене на валутните курсове за USD през 2024 година:

```console
$ ./BNB_downloader.py USD 2024 USD_2024_corrected.csv
```
За някои валути, например JPY, БНБ дава валутния курс не за една валутна единица, а за повече (в случая с JPY - 100 валутни единици). Скриптът коригира валутния курс така че да бъде за 1 валутна единица.

Скриптът прави проверки на първия ред и хедъра (хедърът е на втория ред, lol), а също и проверка на трибуквения код на валутата на всеки ред с валутни курсове.

# Електронни таблици

Във файловете са вградени валутните курсове за няколко популярни валути. Може да добавите нова валута просто като добавите нов sheet със заглавие трибуквения код на валутата. Автоматичното изчисление няма да работи ако пропуснете да форматирате правилно колоните с датата и валутните курсове. За да не излизат датите като стрингове (със символ ' в началото) трябва при импорта да се зададе тип на колоната "дата (DMY)":

Column type: Date (DMY)

![снимка на екрана](screenshots/import-column-type-date.png) 

Не забравяйте, че някои валутни курсове са не за 1 единица, а за 10, 100 или 1000 валутни единици (проверете на сайта на БНБ кои са тези валути). Тоест числата от втората колона трябва да се разделят на 10, 100 или 1000 за да се получи валутния курс за 1 валутна единица.

## Електронна таблица за изчисляване на доход в друга валута

Електронната таблица currency_conversion_calculator.ods работи с LibreOffice (тествах я с LibreOffice 24.8.3.2).

Предназначена е за прости изчисления (напр. доходи от лихви).

![снимка на екрана на currency_conversion_calculator.ods](screenshots/calculator-screenshot.png) 

Потребителят въвежда датата в желания от него формат (ползва се функцията на електронната таблица за форматиране на дата), трибуквения код на валутата (USD, EUR, GBP и т.н.) и сумата в тази валута. Таблицата автоматично взима правилния валутен курс и изчислява общата сума на дохода в лева.

След като потребителят въведе данните (преди да пусне макроса) трябва да изключи защитата от Tools -> Protect sheet.

Желателно е да изключвате защитата само след като сте приключили с въвеждането на данните (за да не се объркате да въведете данни там, където те ще се изгубят при пускане на макроса) и да я пускате след
като макросът приключи.

Макросът се пуска от Tools -> Macros -> Run macro -> RUN_ME -> Run.

Ако не виждате RUN_ME (след като изберете Run macro от Library) отворете падащото меню от настоящия файл, отворете Standard и от него изберете Module 1.

След като приключи изпълнението на макроса ще излезе съобщение.

## Електронна таблица за дивиденти

Електронната таблица dividends_calculator.ods работи с LibreOffice (тествах я с LibreOffice 24.8.3.2).

Предназначена е за доходи от дивиденти. Ако има удържан дивидент приема, че е код 1 (обикновен данъчен кредит), в противен случай приема код 3. (Което формално не е правилно, но не е съществен недостатък, сложно е за обяснение, за подробности - вижте страницата на [nap-autopilot](https://github.com/vstoykovbg/nap-autopilot), където го обясних).

Ползва се една и съща дата за превалутиране както на получения дивидент, така и на удържания данък. Не съм виждал случай когато данъкът е удържан на дата, която е различна от датата на дивидента.

Изчислява повече колони, отколкото са необходими за [nap-autopilot](https://github.com/vstoykovbg/nap-autopilot) (тоест може ако желаете ръчно да препишете съответните числа в годишната данъчна декларация, ако по някаква причина не искате да ползвате nap-autopilot).

![снимка на екрана на dividends_calculator.ods](screenshots/dividends_calculator_screenshot.png) 

Преди да пуснете макроса изключете защитата на **dividends_calculator** от Tools -> Protect sheet.

Обърнете внимание, че преди да изберете Tools -> Protect sheet трябва да сте избрали dividends_calculator, а не "Инструкции за употреба".

Желателно е да изключвате защитата само след като сте приключили с въвеждането на данните (за да не се объркате да въведете данни там, където те ще се изгубят при пускане на макроса) и да я пускате след като макросът приключи.

Макросът се пуска от Tools -> Macros -> Run macro -> RUN_ME -> Run.

Ако не виждате RUN_ME (след като изберете Run macro от Library) отворете падащото меню от настоящия файл, отворете Standard и от него изберете Module 1.

След като приключи изпълнението на макроса ще излезе съобщение.

RUN_ME_with_cleaning прави същото като RUN_ME с тази разлика, че изчиства редовете на колони F, G, H, I, J, K, L, M, N до 100 реда надолу от последната дата в колона E (с цел оптимизация е ограничена проверката за въведени данни). Ако има празнина от повече от 100 реда в колона E следващите редове се игнорират.

След изпълнение на макроса можете да запазите **dividends_export_csv** във формат CSV с цел да захраните [nap-autopilot](https://github.com/vstoykovbg/nap-autopilot) (автоматично въвеждане на данни в данъчната декларация).

## Електронна таблица за притежавани акции (или дялове)

Електронната таблица shares_calculator.ods работи с LibreOffice (тествах я с LibreOffice 24.8.3.2).

Предназначена е за подпомагане попълването на таблиците в приложение 8 за притежаваните акции и дялове. Подпомага генерирането на CSV файл за nap-autopilot.

![снимка на екрана на shares_calculator.ods](screenshots/shares_calculator_screenshot.png) 

Преди да пуснете макроса изключете защитата на shares_calculator от Tools -> Protect sheet.

Обърнете внимание, че преди да изберете Tools -> Protect sheet трябва да сте избрали shares_calculator, а не "Инструкции за употреба".

Желателно е да изключвате защитата само след като сте приключили с въвеждането на данните (за да не се объркате да въведете данни там, където те ще се изгубят при пускане на макроса) и да я пускате след като макросът приключи.

Въвеждането на наименование и ISIN код не е задължително (но е полезно при проверка дали правилно сте въвели данните и за евентуално попълване на СПБ-8).

Макросът се пуска от Tools -> Macros -> Run macro -> RUN_ME -> Run.

Ако не виждате RUN_ME (след като изберете Run macro от Library) отворете падащото меню от настоящия файл, отворете Standard и от него изберете Module 1.

След като приключи изпълнението на макроса ще излезе съобщение.

RUN_ME_with_cleaning прави същото като RUN_ME с тази разлика, че изчиства редовете на колони F, G, J, K до 100 реда надолу от последната дата в колона E (с цел оптимизация е ограничена проверката за въведени данни). Ако има празнина от повече от 100 реда в колона E следващите редове се игнорират.

След изпълнение на макроса можете да запазите shares_export_csv във формат CSV с цел да захраните [nap-autopilot](https://github.com/vstoykovbg/nap-autopilot) (автоматично въвеждане на данни в данъчната декларация).

## Електронна таблица доходи с код 508 (опростена, изчисляваща правилно само ако въведените данни отговарят на определени критерии)

Електронната таблица sales_calculator_simple.ods работи с LibreOffice (тествах я с LibreOffice 24.8.3.2).

Предназначена е за доходите с код 508. Подпомага генерирането на CSV файл за nap-autopilot.

> [!WARNING]
> Внимание! Запознайте се с логиката на изчисления в този калкулатор за да разберете какви са му ограниченията.

* Калкулаторът няма да изведе коректен резултат ако въведете покупки на активи, които не са продадени изцяло през данъчната година, която е обект на изчисления.
* Ако има въведени продажби през други данъчни години резултатът от изчисленията няма да е правилен.
* Този калкулатор работи правилно само при условие, че въведените покупки се отнасят изцяло за въведените продажби.

Допустимо е за някой идентификатор да има въведена само продажба (например ако има продадени опции или е продадено нещо, за което няма документи за придобиване. Съгласно ЗДДФЛ цената на придобиване в този случай е нула (документално доказаната цена е нула при липса на документи, удостоверяващи цената на придобиване).

Ако за някой идентификатор няма въведени продажби той се игнорира.

Нарочно не употребявам израза данъчен лот, а уникален идентификатор, защото калкулаторът работи правилно и когато няма данъчни лотове, но само ако условията са изпълнени.

**Тип сделки, които калкулаторът ще обработи правилно:**

1. Прости покупко-продажби - една покупка и една продажба като с продажбата се продава изцяло тове, което е купено (и продажбата е направена през годината, за която се отнасят изчисленията).
2. Сложни покупко-продажби (покупка/покупки, последвани от продажба/продажби), но позициите са изцяло затворени през данъчната година, за която се отнасят изчисленията.   

Какво значи изцяло затворена позиция? Пример: купуваме 10 акции, после още 10. Имаме дълга позиция от 20 акции. През 2024 година (за която правим изчисленията на доходите) продаваме 15 акции. Това не е изцяло затворена позиция. Продаваме още 5 акции през 2024 година - с това затваряме изцяло позицията (продали сме всичко което сме купили).

Пример за stock split: Няма проблем ако е имало stock split, например купили сме 20 акции, след сплит (акциите са се разцепили на 10) имаме 200 акции. Ако изцяло продадем тези 200 акции през данъчната година, за която се отнасят изчисленията (и правилно сме въвели цената на придобиване на 20-те акции), калкулаторът ще изведе коректен резултат. В калкулатора не е предвидено да се въвежда брой на акциите.

**Тип сделки, които калкулаторът няма да обработи правилно:**

1. Продажба (или продажби) през предишна данъчна година.
   (Тоест дългите позиции, описани като покупки, са затворени напълно или частично през предишна данъчна година, а не през данъчната година, за която се отнасят изчисленията.)
3. Покупки на активи, които не са продадени изцяло през данъчната година, за която се правят изчисленията.
   (Тоест дългите позиции, описани като покупки, не са затворени или не са затворени напълно.)

Пример за ненапълно затворена позиция (грешно ползване на калкулатора): Купили сме 10 акции през 2023 година и сме написали цената на придобиване на тези 10 акции в калкулатора като покупка. Продаваме 5 акции през 2024 година (за която се отнасят изчисленията) и въвеждаме продажната цена на тези 5 акции в калкулатора като продажба. Резултатът ще бъде грешен, защото не е коректно да вадим от продажната цена на 5 акции цената на придобиване на 10 акции. За да бъдат изчисленията коректни трябва да сметнем отделно (в друга таблица) колко е цената на придобиване на 5 от акциите и да напишем нея в калкулатора като покупка. Разбира се, пишем цената на придобиване на 5 от акциите в оригиналната валута и датата, а калкулаторът сам намира валутния курс и го въвежда на съответния ред за да изчисли сумата в лева.

Пример за продажба през предишна данъчна година (грешно ползване на калкулатора):  Купили сме 10 акции през 2023 година и сме написали цената на придобиване на тези 10 акции в калкулатора като покупка. Продаваме 5 акции през 2023 година и 5 акции през 2024 година (годината, за която сте отнасят изчисленията). Калкулаторът ще даде грешен резултат. За да бъдат изчисленията коректни трябва да направим същото като в предишния преимер - да сметнем отделно (в друга таблица) колко е цената на придобиване на 5 от акциите и да напишем нея в калкулатора като покупка. (Защо 5 от акциите? Защото продаваме 5 акции и трябва да напишем цената на придобиване на 5 акции за да бъдат коректни изчисленията.) Разбира се, пишем цената на придобиване на 5 от акциите в оригиналната валута и датата, а калкулаторът сам намира валутния курс и го въвежда на съответния ред за да изчисли сумата в лева. Продажбата от 2023 година (година, която се пада предишна на данъчната година, за която правим изчисленията) не я пишем в калкулатора.

Тоест при продажба на 5 акции през данъчната година, за която правим изчисленията, трябва да напишем цената на придобиване на същия брой акции (нито повече, нито по-малко). Как се определя цената на придобиване на порцията, която продаваме, може да бъде просто, но може и да е сложно (ако сме правили по сложен начин покупки и продажби).

Пример за сложни покупко-продажби, с които този калкулатор не може да се справи (без сериозна допълнителна обработка на данните с друг инструмент): През предишната данъчна година: покупка на 10 акции, покупка на 5 акции, продажба на 3 акции, покупка на 2 акции, продажба на 6 акции. През настоящата данъчна година: покупка на 4 акции, продажба на 2 акции, покупка на 7 акции, продажба на 12 акции. Ако е имало stock split и/или сливане/отделяне на компании в някой междинен момент това допълнително ще усложни изчисленията.

...

![снимка на екрана на sales_calculator_simple.ods](screenshots/sales_calculator_simple_screenshot.png) 

Данни от потребителя се въвеждат само на sales_calculator_simple. 

Преди да пуснете макроса изключете защитата на sales_calculator_simple от Tools -> Protect sheet.

Обърнете внимание, че преди да изберете Tools -> Protect sheet трябва да сте избрали sales_calculator_simple, а не този sheet (Инструкции за употреба).

Желателно е да изключвате защитата само след като сте приключили с въвеждането на данните (за да не се объркате да въведете данни там, където те ще се изгубят при пускане на макроса) и да я пускате след като макросът приключи.

Въвеждането на наименование и ISIN код не е задължително.
 
Макросът се пуска от Tools -> Macros -> Run macro -> RUN_ME -> Run.

Ако не виждате RUN_ME (след като изберете Run macro от Library) отворете падащото меню от настоящия файл, отворете Standard и от него изберете Module 1.

След като приключи изпълнението на макроса ще излезе съобщение.

RUN_ME_with_cleaning прави същото като RUN_ME с тази разлика, че изчиства редовете на колони F, G, J, K до 100 реда надолу от последната дата в колона E (с цел оптимизация е ограничена проверката за въведени данни). Ако има празнина от повече от 100 реда в колона E следващите редове се игнорират. Изчистват се по подобен начин и колоните A, B, C, D, E  от „Резултати“.

След изпълнение на макроса можете да запазите sales_export_csv във формат CSV с цел да захраните [nap-autopilot](https://github.com/vstoykovbg/nap-autopilot) (автоматично въвеждане на данни в данъчната декларация).


(Обяснението за стоте реда е силно опростено, всъщност алгоритъмът работи с блокове от 100 реда, така че редовете надолу не са точно 100.)

-----

## Подводни камъни при четене на справките и грешни данни

При някои инвестиционни посредници справките в PDF формат не могат да се обработят даже и с изкуствен интелект. Например при TastyTrade - те дават машинно четими справки със закъснение (не съм ги проверявал дали са годни, но предишна година имаше грешна дата в една от машинно четимите им справки и затова внимавам).

Прочетете първо за подводните камъни при четенето на справки, има различни видове грешки в някои справки (вижте [статията ми за Interactive Brokers](https://redtapepayments.blogspot.com/2021/08/interactive-brokers-2021.html) и за [попълването на данъчната декларация](https://redtapepayments.blogspot.com/2020/10/blog-post_4.html)). Подробно обясних в цитираната статия защо смятам, че за данъчни цели (за изчисляване на печалбата от поскъпването) се взима trade date, а не settlement date.

Interactive Brokers съм ги хващал да слагат грешна дата в данъчната справка за дивидентите (обясних в коя справка е коректната дата в цитираните статии) и да не слагат някои от дивидентите в общата справка когато те са получени съвсем в края на годината (за да се видят трябва да се избере като крайна дата някой работен ден от януари следващата година - затова си вадя контролни справки с начална дата няколко дена преди началото на годината и няколко дена след края ѝ за да проверя дали има изпуснати данни).

## Капани при обработката на данните

НАП работи с десетични точки, не десетични запетаи. Внимавайте във формата на числата.

Не забрайвяте във формулата за преизчисляване да сложите закръгляване до втория знак след десетичната точка - например `=ROUND(A3*C3,2)`.

![снимка на екрана на електронна таблица с формула за закръгляване](https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEhH91jrvB0x5pLP-ozaNdTI6i8mTTh4ufCDsNEYzFUdaD4bDKz2y_P5Gbf6F7B9MMuIBnMKYRZXpVc_nY65YOlj-hKur2WoVxiPeLlGJKLS8e9JTaK2GLGm_mJLNtfNznRmQ00t0E0fO3KfHJyyd8CB-Pi8_ekyqCj7JkptN6MfEJS_n2FMPvs2TsCZoAWO/s409/interest-example.png) 

Подробно как трябва да се обработят данните за данъчната декларация:

* [Отговори на въпроси за данъците и попълване на годишната данъчна декларация при инвестиции на финансовите пазари](https://redtapepayments.blogspot.com/2020/10/blog-post_4.html)

За дивидентите и лихвите сравнително лесно може да се автоматизира целия процес (ако справките не са в някой неподходящ за машинна обработка PDF формат като на TastyTrade). Но за капиталовата печалба има усложнения с данъчните лотове и определяне коя продажба трябва да се включи в изчисленията и коя не трябва (липват данни в справките за това дали сделката е минала на регулиран пазар или не, трябва да се вадят данни и от сайта на ESMA за [да се провери на кой сегмент от борсата е минала сделката и дали този сегмент е вписан като regulated market или не](https://redtapepayments.blogspot.com/2024/02/blog-post.html)). Има усложнения с данъчните лотове и при данните за притежавани акции и дялове към 31 декември.

В справките на Degiro има данни за MIC кода на сегмента, през който е минала сделката. Такива данни няма в спраквите на Interactive Brokers. Освен това в справките от Interactive Brokers идентификаторите на борсите не са стандартни MIC кодове и се налага да се проверява в сайта на Interactive Brokers кой идентификатор на коя борса съответства. Правете разлика между MIC код на борса и MIC код на сегмент на борса.

-----

## Конвертиране на датата във формат за данъчната декларация и поставяне на валутните курсове в съседна колона

С тези скриптове отчасти се автоматизира обработката на данните за данъчната декларация.

Въвеждането на данните в данъчната декларация може да стане чрез [nap-autopilot](https://github.com/vstoykovbg/nap-autopilot) (захранва се с csv файлове с обработени данни).

Първо вкарваме данните в електронна таблица.

С различни методи проба-грешка се установява кой метод на копиране работи:
* Дали с copy/paste или селектиране и средния бутон (в Linux).
* С кой браузър да се отварят HTML справките за да работи коректно копирането.
* С кой браузър или програма за преглед на PDF да се отварят справките във формат PDF за да работи копирането от PDF.
* Дали първо да се копира в обикновен текстов редактор и после от там да се копира и постави (copy/paste) в електронната таблица.
* Как да се постави (paste) - дали да се избере нещо от менюто Paste Special или не.

В Interactive Brokers има възможност справката да е в CSV формат (освен HTML и PDF), което услеснява прехвърлянето на данни в електронна таблица.

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

Данните на входа са във формата, който е ползван в справките от TastyTrade в PDF формат (от където преписвам ръчно в електронна таблица данните за доходите):

Примерни данни за `input_file`:
```
05/25/23
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

Данните на входа от следващия пример са във формата, който е ползван в справките от Interactive Brokers в HTML формат (от където копирам в електронна таблица данните за доходите): месец/ден/година.

Примерни данни за `input_file`:
```
2023-01-03
2023-02-01
2023-03-10
2023-03-17
2023-03-31
2023-03-31
2023-04-03
2023-05-01
2023-06-12
2023-07-05
2023-08-01
```
На изхода получаваме във файла `output_file.csv`:

```
Original Date,Converted Date,Currency Rate
2023-01-03,03.01.2023,1.85475
2023-02-01,01.02.2023,1.79533
2023-03-10,10.03.2023,1.84756
2023-03-17,17.03.2023,1.84113
2023-03-31,31.03.2023,1.79846
2023-03-31,31.03.2023,1.79846
2023-04-03,03.04.2023,1.79929
2023-05-01,01.05.2023,1.7811
2023-06-12,12.06.2023,1.81684
2023-07-05,05.07.2023,1.7978
2023-08-01,01.08.2023,1.78289
```
