#!/usr/bin/perl

use strict;
use warnings;

use DBI;

# имя файла почтового лога
my $file_name = 'out';

# значения для соединения с БД
my $db_name = 'medweb';
my $db_user = 'src';
my $db_password = '';

# соединяемся с БД
my $dbh = DBI->connect("dbi:Pg:dbname=$db_name", $db_user, $db_password,
    {AutoCommit => 0, RaiseError => 1, PrintError => 1, pg_server_prepare => 1});

# готовим запросы на вставку
my $sth_message = $dbh->prepare('insert into message(created, int_id, id, str) values(?, ?, ?, ?)');
my $sth_log = $dbh->prepare('insert into log(created, int_id, address, str) values(?, ?, ?, ?)');
my $sth_log_error = $dbh->prepare('insert into log_error(created, int_id, str) values(?, ?, ?)');

# открываем и читаем почтовый лог построчно
open(my $file_handle, '<', $file_name) or die "cannot open file $file_name";
while (my $line = <$file_handle>) {
  # убираем символ конца строки
  chomp($line);

  # выделяем дату, время и строку лога без временной метки
  my ($date, $time, $str) = split(' ', $line, 3);

  # формируем временнУю метку
  my $created = $date.' '.$time;

  # пытаемся выделить из str значения int_id и flag
  my ($int_id, $flag, $other_info) = split(' ', $str, 3);

  # значение int_id определено всегда,
  # проверяем, допустимо ли значение flag
  unless (defined($flag) && $flag =~ /(<=|=>|->|\*\*|==)/) {
    # значение поля flag недопустимо => пишем str в log_error и переходим к следующей строке
    $sth_log_error->execute($created, $int_id, $str);
    next;
  }

  # значение flag определяет, в какую таблицу вставлять запись
  if ($flag eq '<=') {
    # запись должна попасть в таблицу message

    # ищем значение поля id=xxxx из строки лога
    my $id;
    if ($other_info =~ /(^| )id=(.+)($| )/) {
      $id = $2;
    }
    unless (defined($id)) {
      # значение поля id не определено => пишем str в log_error и переходим к следующей записи
      $sth_log_error->execute($created, $int_id, $str);
      next;
    }

    # значение str должно быть not null
    $str = '' unless defined $str;

    # вставляем запись в таблицу message
    $sth_message->execute($created, $int_id, $id, $str);
  } else {
    # выделяем из other_info значение address
    my ($address, $other_info2) = split(' ', $other_info, 2);

    # вставляем запись в таблицу log
    $sth_log->execute($created, $int_id, $address, $str);
  }
}

# фиксируем изменения в БД
$dbh->commit();

# закрываем почтовый лог
close($file_handle);

# подводим итоги
my $sth_count = $dbh->prepare('select count(*) from message');
$sth_count->execute();
my ($count) = $sth_count->fetchrow();
print "message count = $count\n";

$sth_count = $dbh->prepare('select count(*) from log');
$sth_count->execute();
($count) = $sth_count->fetchrow();
print "log count = $count\n";

$sth_count = $dbh->prepare('select count(*) from log_error');
$sth_count->execute();
($count) = $sth_count->fetchrow();
print "log_error count = $count\n";

# закрываем хэндл запроса
$sth_count->finish();

# отсоединяемся от БД
$dbh->disconnect();

exit(0);
