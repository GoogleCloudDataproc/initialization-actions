#!/usr/bin/perl -w
# -*-CPerl-*-

# Usage: Run this script from the root directory of the git clone:
# perl templates/generate-action.pl gpu/install_gpu_driver.sh

use Template;
use strict;
use v5.10;

my $tt = Template->new( {
  INCLUDE_PATH => "$ENV{PWD}/templates",
  INTERPOLATE  => 0,
}) || die "$Template::ERROR$/";

my $action = $ARGV[0];

sub usage{
  die "Usage: $0 <action>";
}

usage unless( -f "$ENV{PWD}/templates/${action}.in" );

$tt->process("${action}.in")
    || die $tt->error(), "\n";
