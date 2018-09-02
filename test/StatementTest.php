<?php namespace Spoom\Sql\MySQL;

use PHPUnit\Framework\TestCase;
use Spoom\Sql\Expression;

/**
 * Class StatementTest
 *
 * TODO cover the advanced usages and the DELETE command
 */
class StatementTest extends TestCase {

  /**
   * @param Expression\Statement $statement
   *
   * @dataProvider providerStatement
   */
  public function testSelect( Expression\Statement $statement ) {

    // test field manipulation
    $statement->setField( [ 'fieldwithoutalias', 'fieldwithalias_alias' => 'fieldwithalias', 'fieldwithcontext' ] );
    $statement->getContext()[ 'field' ] = [ [ 'fieldwithcontext' => 10 ] ];
    $this->assertEquals( '(SELECT fieldwithoutalias, fieldwithalias AS `fieldwithalias_alias`, 10 AS `fieldwithcontext`)', (string) $statement );
    $statement->removeField( [ 'fieldwithalias_alias', 'fieldwithcontext' ] );
    $this->assertEquals( '(SELECT fieldwithoutalias)', (string) $statement );

    // test table manipulation
    $statement->addTable( 'tablewithoutjoin' )->addTable( 'tablewithjoin', 'tablewithjoin_alias', 'tablewithjoin_alias.a = tablewithoutjoin.b' );
    $this->assertEquals( '(SELECT fieldwithoutalias FROM tablewithoutjoin INNER JOIN tablewithjoin AS `tablewithjoin_alias` ON tablewithjoin_alias.a = tablewithoutjoin.b)', (string) $statement );
    $statement->removeTable( [ 'tablewithjoin_alias' ] );
    $this->assertEquals( '(SELECT fieldwithoutalias FROM tablewithoutjoin)', (string) $statement );

    // test filter manipulation
    $statement->addFilter( '{!field} = {test} OR {test1} = 20', [ 'test' => 10, 'test1' => 20, 'field' => 'fieldwithoutalias' ] )
              ->addFilter( '{test2.test} = 30', [ 'test2' => [ 'test' => 30 ] ] );
    $this->assertEquals( '(SELECT fieldwithoutalias FROM tablewithoutjoin WHERE (`fieldwithoutalias` = 10 OR 20 = 20) AND (30 = 30))', (string) $statement );
    $statement->removeFilter( Expression\Statement::FILTER_SIMPLE, '{test2.test} = 30', [ 'test2' ] );
    $this->assertEquals( '(SELECT fieldwithoutalias FROM tablewithoutjoin WHERE (`fieldwithoutalias` = 10 OR 20 = 20))', (string) $statement );
    $this->assertNull( $statement->getContext()[ Expression\Statement::FILTER_SIMPLE . '.' . Expression\Statement::FILTER_SIMPLE . '.test2' ] );

    // test limit
    $statement->setLimit( 1000 );
    $this->assertEquals( '(SELECT fieldwithoutalias FROM tablewithoutjoin WHERE (`fieldwithoutalias` = 10 OR 20 = 20) LIMIT 1000)', (string) $statement );
    $statement->setLimit( 1000, 100 );
    $this->assertEquals( '(SELECT fieldwithoutalias FROM tablewithoutjoin WHERE (`fieldwithoutalias` = 10 OR 20 = 20) LIMIT 100, 1000)', (string) $statement );
    $statement->setLimit( 100 );
    $this->assertEquals( '(SELECT fieldwithoutalias FROM tablewithoutjoin WHERE (`fieldwithoutalias` = 10 OR 20 = 20) LIMIT 100, 100)', (string) $statement );
  }

  /**
   * @param Expression\Statement $statement
   *
   * @dataProvider providerStatement
   */
  public function testInsert( Expression\Statement $statement ) {

    // test mass insert
    $statement->setField( [ 'fieldwithoutalias', 'fieldwithalias_alias' => 'fieldwithalias', 'fieldwithcontext' ] );
    $statement->getContext()[ 'field' ] = [ [ 'fieldwithoutalias' => 10, 'fieldwithcontext' => 20 ], [ 'fieldwithalias_alias' => 300, 'fieldwithoutalias' => 100 ] ];
    $statement->addTable( 'tablewithjoin', 'tablewithjoin_alias', 'tablewithjoin_alias.a = tablewithoutjoin.b' )->addTable( 'tablewithoutjoin' );

    $this->assertEquals(
      '(INSERT tablewithoutjoin(`fieldwithoutalias`,`fieldwithalias_alias`,`fieldwithcontext`) VALUES(10,fieldwithalias,20),(100,fieldwithalias,DEFAULT))',
      '(' . $statement->getConnection()->apply( $statement->getInsert(), $statement->getContext() ) . ')'
    );
  }

  /**
   * @param Expression\Statement $statement
   *
   * @dataProvider providerStatement
   */
  public function testUpdate( Expression\Statement $statement ) {

    // test field render in an update
    $statement->setField( [ 'fieldwithoutalias', 'fieldwithalias_alias' => 'fieldwithalias', 'fieldwithcontext' ] );
    $statement->getContext()[ 'field' ] = [ [ 'fieldwithoutalias' => 10, 'fieldwithalias_alias' => 30, 'fieldwithcontext' => 20 ] ];
    $statement->addTable( 'tablewithjoin', 'tablewithjoin_alias', 'tablewithjoin_alias.a = tablewithoutjoin.b' )->addTable( 'tablewithoutjoin' );

    $this->assertEquals(
      '(UPDATE tablewithoutjoin INNER JOIN tablewithjoin AS `tablewithjoin_alias` ON tablewithjoin_alias.a = tablewithoutjoin.b SET `fieldwithoutalias` = 10, `fieldwithalias_alias` = fieldwithalias, `fieldwithcontext` = 20)',
      '(' . $statement->getConnection()->apply( $statement->getUpdate(), $statement->getContext() ) . ')'
    );
  }

  /**
   * @return array
   */
  public function providerStatement() {
    return [
      [ ( new Connection( 'database', 'root', '', 'test_db', [
        'encoding' => 'utf8',
        'timezone' => 'UTC'
      ] ) )->statement() ]
    ];
  }
}
