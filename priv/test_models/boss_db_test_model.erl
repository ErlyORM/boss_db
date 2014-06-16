-module(boss_db_test_model, [Id, SomeText, SomeTime, SomeBoolean, SomeInteger, SomeFloat, BossDbTestParentModelId]).
-compile(export_all).

-belongs_to(boss_db_test_parent_model).

%% This is necessary when using MySQL due to MySQL not supporting a real
%% BOOLEAN type
attribute_types() -> [{some_boolean, boolean}].
