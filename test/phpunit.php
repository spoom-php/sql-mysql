<?php require __DIR__ . '/../vendor/autoload.php';

use Spoom\Core\Application;
use Spoom\Core\File;

// setup the Spoom application 
$spoom = new Application(
  Application::ENVIRONMENT_TEST,
  'en',
  new File( __DIR__ . '/../' )
);
