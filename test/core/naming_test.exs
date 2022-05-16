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
end
