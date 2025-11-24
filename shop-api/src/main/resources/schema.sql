
create table shop (
    id bigint primary key auto_increment,
    identifier varchar(100) not null,
    buyer_identifier varchar(100) not null,
    status varchar(20) not null,
    date_shop date
);

create table shop_item (
    id bigint primary key auto_increment,
    product_identifier varchar(100) not null,
    amount int not null,
    price float not null,
    shop_id bigint not null references shop(id)
);


