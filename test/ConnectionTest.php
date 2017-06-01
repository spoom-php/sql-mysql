<?php namespace Spoom\Sql\MySQL;

use PHPUnit\Framework\TestCase;
use Spoom\Sql\ResultInterface;

class ConnectionTest extends TestCase {

  /**
   * @param Connection $connection
   *
   * @dataProvider providerConnection
   */
  public function testConnect( Connection $connection ) {

    // test connecting
    $this->assertFalse( $connection->isConnected() );
    $connection->connect();
    $this->assertTrue( $connection->isConnected() );

    // test user change
    $connection->setAuthentication( 'test_user', 'test_user-pass' );

    // test disconnecting
    $connection->disconnect();
    $this->assertFalse( $connection->isConnected() );
  }

  /**
   * @param Connection $connection
   *
   * @dataProvider providerConnection
   * @depends      testConnect
   */
  public function testExecute( Connection $connection ) {

    // test single statement
    $result = $connection->execute( 'CREATE TABLE IF NOT EXISTS {!table}( {!column.id} int, {!column.title} varchar(255) )', [
      'table'  => 'test',
      'column' => [
        'id'    => 'id',
        'title' => 'title'
      ]
    ] );

    $this->assertInstanceOf( ResultInterface::class, $result );
    $this->assertNull( $result->getException() );

    // test multi statement
    $result = $connection->execute( [
      'TRUNCATE `test`',
      'INSERT INTO `test`(title) VALUES{0}',
      'INSERT INTO `test`(title) VALUES{1}'
    ], [
      [ [ 'a' ], [ 'b' ], [ 'c' ] ],
      [ [ 'd' ], [ 'e' ] ]
    ] );

    $this->assertTrue( is_array( $result ) );
    $this->assertEquals( 3, count( $result ) );
    $this->assertEquals( [ 'a', 'b', 'c', 'd', 'e' ], $connection->execute( 'SELECT title FROM `test` ORDER BY `title`' )->getList() );
  }

  /**
   * @return array
   */
  public function providerConnection() {
    return [
      [ new Connection( 'database', 'root', '', 'test_db', [
        'encoding' => 'utf8',
        'timezone' => 'UTC'
      ] ) ]
    ];
  }
}
