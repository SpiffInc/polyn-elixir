defmodule Polyn.NamingTest do
  use ExUnit.Case, async: true

  alias Polyn.Naming

  test "dot_to_colon/1" do
    assert "com:acme:user:created:v1:schema:v1" ==
             Naming.dot_to_colon("com.acme.user.created.v1.schema.v1")
  end

  test "colon_to_dot/1" do
    assert "com.acme.user.created.v1.schema.v1" ==
             Naming.colon_to_dot("com:acme:user:created:v1:schema:v1")
  end

  describe "trim_domain_prefix/1" do
    test "removes prefix when dots" do
      assert "user.created.v1.schema.v1" ==
               Naming.trim_domain_prefix("com.test.user.created.v1.schema.v1")
    end

    test "removes prefix when colon" do
      assert "user:created:v1:schema:v1" ==
               Naming.trim_domain_prefix("com:test:user:created:v1:schema:v1")
    end

    test "only removes first occurence" do
      assert "user.created.com.test.v1.schema.v1" ==
               Naming.trim_domain_prefix("com.test.user.created.com.test.v1.schema.v1")
    end
  end

  test "version_suffix/1 defaults to version 1" do
    assert Naming.version_suffix("com:acme:user:created:") ==
             "com:acme:user:created:v1"
  end

  test "version_suffix/2 adds version" do
    assert Naming.version_suffix("com:acme:user:created:", 2) ==
             "com:acme:user:created:v2"
  end

  describe "trim_version_suffix/1" do
    test "with dots" do
      assert Naming.trim_version_suffix("com.acme.user.created.v1") == "com.acme.user.created"
    end

    test "with colons" do
      assert Naming.trim_version_suffix("com:acme:user:created:v1") == "com:acme:user:created"
    end
  end

  describe "validate_event_name/1" do
    test "valid names that's alphanumeric and dot separated passes" do
      assert Naming.validate_event_name("user.created") == :ok
    end

    test "name can't have spaces" do
      assert Naming.validate_event_name("user   created") ==
               {:error, "Event names must be lowercase, alphanumeric and dot separated"}
    end

    test "name can't have tabs" do
      assert Naming.validate_event_name("user\tcreated") ==
               {:error, "Event names must be lowercase, alphanumeric and dot separated"}
    end

    test "name can't have linebreaks" do
      assert Naming.validate_event_name("user\n\rcreated") ==
               {:error, "Event names must be lowercase, alphanumeric and dot separated"}
    end

    test "names can't have special characters" do
      assert Naming.validate_event_name("user:*%[]<>$!@#-_created") ==
               {:error, "Event names must be lowercase, alphanumeric and dot separated"}
    end

    test "names can't start with a dot" do
      assert Naming.validate_event_name(".user") ==
               {:error, "Event names must be lowercase, alphanumeric and dot separated"}
    end

    test "names can't end with a dot" do
      assert Naming.validate_event_name("user.") ==
               {:error, "Event names must be lowercase, alphanumeric and dot separated"}
    end
  end
end
