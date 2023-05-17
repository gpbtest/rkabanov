#!/usr/bin/perl

use strict;
use warnings;

use CGI;
use DBI;
use HTML::Entities;

# имя файла шаблона
use constant TEMPLATE_FILE_NAME => '/www/test/search_log.tmpl';

# значения для соединения с БД
use constant DB_NAME => 'dbase';
use constant DB_USER => 'dbuser';
use constant DB_PASSWORD => '';

# получаем CGI параметры
my $cgi = CGI->new();
my $address = define( $cgi->param('address') );
my $submit = define( $cgi->param('submit') );

# соединяемся с БД
my $dbh = DBI->connect('dbi:Pg:dbname='.DB_NAME, DB_USER, DB_PASSWORD,
    {AutoCommit => 0, RaiseError => 1, PrintError => 1, pg_server_prepare => 1});

# результат поиска
my $result = '';
# сообщение пользователю
my $result_message = '';

if ($submit ne '') {
  # форма отправлена => определяем число найденных записей
  my $records_count = get_records_count($dbh, $address);

  # проверяем ограничение на число записей
  if ($records_count > 100) {
    # слишком много записей => сообщить пользователю
    $result_message = 'Количество найденных строк больше 100';
  } elsif ($records_count > 0) {
    # укладываемся в лимит => получаем данные
    my $records = get_records($dbh, $address);

    # форматируем данные
    $result = format_records($records);
  }

  # если уложились в лимит, но ничего не нашли, сообщить пользователю
  if ($result eq '' && $result_message eq '') {
    $result_message = 'Нет данных';
  }
}

# отсоединяемся от БД
$dbh->disconnect();


# читаем и заполняем шаблон HTML страницы
my $template = get_template(TEMPLATE_FILE_NAME);
$template =~ s/{ADDRESS}/$address/g;
$template =~ s/{SEARCH_RESULT}/$result/g;
$template =~ s/{RESULT_MESSAGE}/$result_message/g;

# выводим HTML
print "Content-Type: text/html\n\n";
print $template;

exit(0);


# ------------------------------------------
sub define
{
  # вспомогательная функция: сделать значение определённым (defined)

  # IN: $s - исходное значение
  # RETURN: исходное значение или пустая строка, если исх. значение не определено

  my $s = shift;
  return defined($s) ? $s : '';
}


sub get_template
{
  # прочитать и вернуть шаблон страницы из заданного файла 

  # IN: строка $file_name - имя файла шаблона
  # RETURN: строка - содержимое шаблона

  my $file_name = shift;

  open(my $fh, '<', $file_name) or die "cannot read template $file_name";

  # читаем файл целиком
  my $template = '';
  my $file_size = -s $file_name;

  read($fh, $template, $file_size);

  return $template;
}


sub get_records_count
{
  # выбрать из БД записи, содержащие в адресе получателя заданную строку

  # IN: $dbh - хэндлер соединения с БД
  # IN: $address - строка поиска
  # RETURN: записи, удовлетворяющие критерию поиска - ссылка на массив хэшей [{created, str}]

  my $dbh = shift;
  my $address = shift;

  my $count = 0;

  # Выбираем записи только из таблицы log, поскольку требуется поиск только по адресу получателя.
  # В таблице message нет данных об адресе получателя.
  # Иначе запрос использовал бы union по таблицам message и log 
  my $sql = q^select count(*)
from log
where address ilike ?^;
  my $sth = $dbh->prepare($sql);

  # преобразуем значение параметра для ilike
  $address = '%'.$address.'%'; 

  my $rv = $sth->execute($address);
  if ($rv) {
    ($count) = $sth->fetchrow();
  }
  $sth->finish();

  return $count;
}


sub get_records
{
  # выбрать из БД записи, содержащие в адресе получателя заданную строку

  # IN: $dbh - хэндлер соединения с БД
  # IN: $address - строка поиска
  # RETURN: записи, удовлетворяющие критерию поиска - ссылка на массив хэшей [{created, str}]

  my $dbh = shift;
  my $address = shift;

  my @result = ();

  # Выбираем записи только из таблицы log, поскольку требуется поиск только по адресу получателя.
  # В таблице message нет данных об адресе получателя.
  # Иначе запрос использовал бы union по таблицам message и log 
  my $sql = q^select created, str
from log
where address ilike ?
order by int_id, created^;
  my $sth = $dbh->prepare($sql);

  # преобразуем значение параметра для ilike
  $address = '%'.$address.'%';

  # выбираем данные и заполняем массив результатов
  my $rv = $sth->execute($address);
  if ($rv) {
    while (my ($created, $str) = $sth->fetchrow()) {
      push(@result, ({ created => $created, str => $str }));
    }
  }
  $sth->finish();

  return \@result;
}


sub format_records
{
  # преобразовать массив записей из лога в строку HTML

  # IN: $records - ссылка на массив хэшей: [{created, str}]
  # RETURN: строка - форматированные и объединенные записи (HTML таблица), или пустая строка

  my $records = shift;

  my $result = '';

  # форматируем каждую запись в строку таблицы HTML: '<timestamp> <строка лога>'
  my @rows = map {
    my $record = $_->{created}.' '.$_->{str};
    '<tr><td>' . HTML::Entities::encode_entities($record) . '</td></tr>'
  } @{$records};

  # объединяем массив строк в одну
  my $tbody = join("\n", @rows);

  # если есть записи, то добавляем тэги для таблицы
  if ($tbody) {
    $result = qq^<table>
  <tbody>
$tbody
  </tbody>
</table>^;
  }

  return $result;
}
