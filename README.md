# Spoom Framework
Spoom is a collection of cooperative libraries (extensions), which you can use to "build" a framework that suits your needs.

## About the MySQL
...

## Installation
Install the latest version with

```bash
$ composer require spoom-php/sql-mysql
```

## Usage
You can create connections and execute commands like this:

```php
<?php require __DIR__ . '/vendor/autoload.php';

use Spoom\Sql\MySQL;

// TODO ...you should create an application first...

// create a (lazy) connection. You can force to connect with $connection->connect()
$connection = new MySQL\Connection( '127.0.0.1:3306', 'root', '', 'test_database' );

// select some items from the table 'test', where foo is 'bar'
$result = $connection->execute( 'SELECT title FROM test WHERE foo = {foo}', [ 'foo' => 'bar' ] );

// the statment can be created in builder style. The code below is equivalent with the above ->execute() but it's universal
// $result = $connection->statement()->addTable( 'test' )->addField( 'title' )->addFilter( 'foo = {foo}' )->search( [ 'foo' => 'bar' ] );

// results used in a loop is equals with $result->getArrayList()
foreach( $result as $i => $item ) {
  echo "{$i}. item's title is '{$item['title']}'\n";
}

```

You can also create connection from the configuration file located in the extension public directory.
The file `spoom/spoom-sql-mysql/configuration/connection.json` should contain something like

```json
{
  "myfancyconnection": {
    "host": "127.0.0.1:3306",
    "user": "root",
    "password": "",
    "database": "test_database",
    
    "option": {}
  }
}
```

and then you are able to do

```php
<?php 

use Spoom\Sql\MySQL;

// note that every instance from the same configuration name will be the exact same object
$connection = MySQL\Connection::instance( 'myfancyconnection' );
```

## License
The Spoom Framework is open-sourced software licensed under the [MIT license](http://opensource.org/licenses/MIT).
