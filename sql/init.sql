CREATE TABLE Article (
  timestamp char(14) DEFAULT NULL,
  id char(7) DEFAULT NULL,
  aid char(7) DEFAULT NULL,
  title char(15) DEFAULT NULL,
  category char(11) DEFAULT NULL,
  abstract char(30) DEFAULT NULL,
  articleTags char(14) DEFAULT NULL,
  authors char(13) DEFAULT NULL,
  language char(3) DEFAULT NULL,
  text char(31) DEFAULT NULL,
  image char(32) DEFAULT NULL,
  video char(32) DEFAULT NULL
);

CREATE TABLE User (
  timestamp char(14) DEFAULT NULL,
  id char(5) DEFAULT NULL,
  uid char(5) DEFAULT NULL,
  name char(9) DEFAULT NULL,
  gender char(7) DEFAULT NULL,
  email char(10) DEFAULT NULL,
  phone char(10) DEFAULT NULL,
  dept char(9) DEFAULT NULL,
  grade char(7) DEFAULT NULL,
  language char(3) DEFAULT NULL,
  region char(10) DEFAULT NULL,
  role char(6) DEFAULT NULL,
  preferTags char(7) DEFAULT NULL,
  obtainedCredits char(3) DEFAULT NULL
);

CREATE TABLE Read (
  timestamp char(14) DEFAULT NULL,
  id char(7) DEFAULT NULL,
  uid char(5) DEFAULT NULL,
  aid char(7) DEFAULT NULL,
  readTimeLength char(3) DEFAULT NULL,
  agreeOrNot char(2) DEFAULT NULL,
  commentOrNot char(2) DEFAULT NULL,
  shareOrNot char(2) DEFAULT NULL,
  commentDetail char(100) DEFAULT NULL
);
